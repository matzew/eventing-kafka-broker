/*
 * Copyright 2021 The Knative Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package channel

import (
	"context"
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/util/retry"
	messagingv1beta1 "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/messaging/v1beta1"
	v1 "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/pkg/apis/duck"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/reconciler"
	"knative.dev/pkg/resolver"

	kafkaclientset "knative.dev/eventing-kafka-broker/control-plane/pkg/client/clientset/versioned"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	coreconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/core/config"
	kafkabrokerlogging "knative.dev/eventing-kafka-broker/control-plane/pkg/logging"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/receiver"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
)

const (
	// TopicPrefix is the Kafka Channel topic prefix - (topic name: knative-channel-<channel-namespace>-<channel-name>).
	TopicPrefix = "knative-channel-"
)

type Configs struct {
	config.Env

	BootstrapServers string
}

type Reconciler struct {
	*base.Reconciler

	Resolver *resolver.URIResolver

	KafkaDefaultTopicDetails     sarama.TopicDetail
	KafkaDefaultTopicDetailsLock sync.RWMutex
	bootstrapServers             []string
	bootstrapServersLock         sync.RWMutex
	ConfigMapLister              corelisters.ConfigMapLister
	endpointsLister              corelisters.EndpointsLister
	kafkaClientSet               kafkaclientset.Interface

	// ClusterAdmin creates new sarama ClusterAdmin. It's convenient to add this as Reconciler field so that we can
	// mock the function used during the reconciliation loop.
	ClusterAdmin kafka.NewClusterAdminFunc

	Configs *Configs

	statusManager *Prober
}

func (r *Reconciler) ReconcileKind(ctx context.Context, channel *messagingv1beta1.KafkaChannel) reconciler.Event {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		return r.reconcileKind(ctx, channel)
	})
}

func (r *Reconciler) reconcileKind(ctx context.Context, channel *messagingv1beta1.KafkaChannel) reconciler.Event {
	logger := kafkabrokerlogging.CreateReconcileMethodLogger(ctx, channel)

	statusConditionManager := base.StatusConditionManager{
		Object:     channel,
		SetAddress: channel.Status.SetAddress,
		Configs:    &r.Configs.Env,
		Recorder:   controller.GetEventRecorder(ctx),
	}

	if !r.IsReceiverRunning() || !r.IsDispatcherRunning() {
		return statusConditionManager.DataPlaneNotAvailable()
	}
	statusConditionManager.DataPlaneAvailable()

	//TODO: topicConfig, channelConfig, err := r.topicConfig(logger, channel)
	var err error = nil
	topicConfig := &kafka.TopicConfig{
		TopicDetail: sarama.TopicDetail{
			NumPartitions:     1,
			ReplicationFactor: 1,
			ReplicaAssignment: nil,
			ConfigEntries:     nil,
		},
		BootstrapServers: []string{"my-cluster-kafka-bootstrap.kafka:9092"},
	}

	if err != nil {
		return statusConditionManager.FailedToResolveConfig(err)
	}
	statusConditionManager.ConfigResolved()

	// TODO
	//if err := r.TrackConfigMap(channelConfig, channel); err != nil {
	//	return fmt.Errorf("failed to track channel config: %w", err)
	//}

	logger.Debug("config resolved", zap.Any("config", topicConfig))

	// TODO
	securityOption := func(config *sarama.Config) error {
		// TODO: noop
		return nil
	}
	// TODO
	var secret *corev1.Secret = nil

	// TODO
	//securityOption, secret, err := security.NewOptionFromSecret(ctx, &security.MTConfigMapSecretLocator{ConfigMap: channelConfig}, r.SecretProviderFunc())
	//if err != nil {
	//	return fmt.Errorf("failed to create security (auth) option: %w", err)
	//}
	//
	//if secret != nil {
	//	logger.Debug("Secret reference",
	//		zap.String("apiVersion", secret.APIVersion),
	//		zap.String("name", secret.Name),
	//		zap.String("namespace", secret.Namespace),
	//		zap.String("kind", secret.Kind),
	//	)
	//}
	//
	//if err := r.TrackSecret(secret, channel); err != nil {
	//	return fmt.Errorf("failed to track secret: %w", err)
	//}

	topic, err := r.ClusterAdmin.CreateTopic(logger, kafka.Topic(TopicPrefix, channel), topicConfig, securityOption)
	if err != nil {
		return statusConditionManager.FailedToCreateTopic(topic, err)
	}
	statusConditionManager.TopicReady(topic)

	logger.Debug("Topic created", zap.Any("topic", topic))

	// Get contract config map.
	contractConfigMap, err := r.GetOrCreateDataPlaneConfigMap(ctx)
	if err != nil {
		return statusConditionManager.FailedToGetConfigMap(err)
	}

	logger.Debug("Got contract config map")

	// Get contract data.
	ct, err := r.GetDataPlaneConfigMapData(logger, contractConfigMap)
	if err != nil && ct == nil {
		return statusConditionManager.FailedToGetDataFromConfigMap(err)
	}

	logger.Debug("Got contract data from config map", zap.Any(base.ContractLogKey, ct))

	// Get resource configuration.
	channelResource, err := r.getChannelResource(ctx, topic, channel, secret, topicConfig)
	if err != nil {
		return statusConditionManager.FailedToGetConfig(err)
	}

	channelIndex := coreconfig.FindResource(ct, channel.UID)
	// Update contract data with the new contract configuration
	changed := coreconfig.AddOrUpdateResourceConfig(ct, channelResource, channelIndex, logger)

	logger.Debug("Change detector", zap.Int("changed", changed))

	if changed == coreconfig.ResourceChanged {
		// Resource changed, increment contract generation.
		coreconfig.IncrementContractGeneration(ct)

		// Update the configuration map with the new contract data.
		if err := r.UpdateDataPlaneConfigMap(ctx, ct, contractConfigMap); err != nil {
			logger.Error("failed to update data plane config map", zap.Error(
				statusConditionManager.FailedToUpdateConfigMap(err),
			))
			return err
		}
		logger.Debug("Contract config map updated")
	}
	statusConditionManager.ConfigMapUpdated()

	// We update receiver and dispatcher pods annotation regardless of our contract changed or not due to the fact
	// that in a previous reconciliation we might have failed to update one of our data plane pod annotation, so we want
	// to anyway update remaining annotations with the contract generation that was saved in the CM.

	// We reject events to a non-existing Channel, which means that we cannot consider a Channel Ready if all
	// receivers haven't got the Channel, so update failures to receiver pods is a hard failure.
	// On the other side, dispatcher pods care about Subscriptions, and the Channel object is used as a configuration
	// prototype for all associated Subscriptions, so we consider that it's fine on the dispatcher side to receive eventually
	// the update even if here eventually means seconds or minutes after the actual update.

	// Update volume generation annotation of receiver pods
	if err := r.UpdateReceiverPodsAnnotation(ctx, logger, ct.Generation); err != nil {
		logger.Error("Failed to update receiver pod annotation", zap.Error(
			statusConditionManager.FailedToUpdateReceiverPodsAnnotation(err),
		))
		return err
	}

	logger.Debug("Updated receiver pod annotation")

	// Update volume generation annotation of dispatcher pods
	if err := r.UpdateDispatcherPodsAnnotation(ctx, logger, ct.Generation); err != nil {
		// Failing to update dispatcher pods annotation leads to config map refresh delayed by several seconds.
		// Since the dispatcher side is the consumer side, we don't lose availability, and we can consider the Channel
		// ready. So, log out the error and move on to the next step.
		logger.Warn(
			"Failed to update dispatcher pod annotation to trigger an immediate config map refresh",
			zap.Error(err),
		)

		statusConditionManager.FailedToUpdateDispatcherPodsAnnotation(err)
	} else {
		logger.Debug("Updated dispatcher pod annotation")
	}

	err = r.reconcileSubscribers(ctx, channel)
	if err != nil {
		return fmt.Errorf("error reconciling subscribers %v", err)
	}

	return statusConditionManager.Reconciled()
}

func (r *Reconciler) FinalizeKind(ctx context.Context, channel *messagingv1beta1.KafkaChannel) reconciler.Event {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		return r.finalizeKind(ctx, channel)
	})
}

func (r *Reconciler) finalizeKind(ctx context.Context, channel *messagingv1beta1.KafkaChannel) reconciler.Event {
	logger := kafkabrokerlogging.CreateFinalizeMethodLogger(ctx, channel)

	// TODO: experimenting
	if channel.Status.Subscribers == nil {
		channel.Status.Subscribers = make([]v1.SubscriberStatus, 0)
	}

	// Get contract config map.
	contractConfigMap, err := r.GetOrCreateDataPlaneConfigMap(ctx)
	if err != nil {
		return fmt.Errorf("failed to get contract config map %s: %w", r.Configs.DataPlaneConfigMapAsString(), err)
	}

	logger.Debug("Got contract config map")

	// Get contract data.
	ct, err := r.GetDataPlaneConfigMapData(logger, contractConfigMap)
	if err != nil {
		return fmt.Errorf("failed to get contract: %w", err)
	}

	logger.Debug("Got contract data from config map", zap.Any(base.ContractLogKey, ct))

	channelIndex := coreconfig.FindResource(ct, channel.UID)
	if channelIndex != coreconfig.NoResource {
		coreconfig.DeleteResource(ct, channelIndex)

		logger.Debug("Channel deleted", zap.Int("index", channelIndex))

		// Resource changed, increment contract generation.
		coreconfig.IncrementContractGeneration(ct)

		// Update the configuration map with the new contract data.
		if err := r.UpdateDataPlaneConfigMap(ctx, ct, contractConfigMap); err != nil {
			return err
		}
		logger.Debug("Contract config map updated")
	}

	// We update receiver and dispatcher pods annotation regardless of our contract changed or not due to the fact
	// that in a previous reconciliation we might have failed to update one of our data plane pod annotation, so we want
	// to update anyway remaining annotations with the contract generation that was saved in the CM.
	// Note: if there aren't changes to be done at the pod annotation level, we just skip the update.

	// Update volume generation annotation of receiver pods
	if err := r.UpdateReceiverPodsAnnotation(ctx, logger, ct.Generation); err != nil {
		return err
	}
	// Update volume generation annotation of dispatcher pods
	if err := r.UpdateDispatcherPodsAnnotation(ctx, logger, ct.Generation); err != nil {
		return err
	}

	// TODO probe (as in #974) and check if status code is 404 otherwise requeue and return.
	//  Rationale: after deleting a topic closing a producer ends up blocking and requesting metadata for max.block.ms
	//  because topic metadata aren't available anymore.
	// 	See (under discussions KIPs, unlikely to be accepted as they are):
	// 	- https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=181306446
	// 	- https://cwiki.apache.org/confluence/display/KAFKA/KIP-286%3A+producer.send%28%29+should+not+block+on+metadata+update

	// TODO:
	//topicConfig, channelConfig, err := r.topicConfig(logger, channel)
	//if err != nil {
	//	return fmt.Errorf("failed to resolve channel config: %w", err)
	//}

	// TODO
	topicConfig := &kafka.TopicConfig{
		TopicDetail: sarama.TopicDetail{
			NumPartitions:     1,
			ReplicationFactor: 1,
			ReplicaAssignment: nil,
			ConfigEntries:     nil,
		},
		BootstrapServers: []string{"my-cluster-kafka-bootstrap.kafka:9092"},
	}

	// TODO
	securityOption := func(config *sarama.Config) error {
		// TODO: noop
		return nil
	}

	// TODO
	//authProvider := &security.MTConfigMapSecretLocator{ConfigMap: channelConfig}
	//securityOption, _, err := security.NewOptionFromSecret(ctx, authProvider, r.SecretProviderFunc())
	//if err != nil {
	//	return fmt.Errorf("failed to create security (auth) option: %w", err)
	//}

	topic, err := r.ClusterAdmin.DeleteTopic(kafka.Topic(TopicPrefix, channel), topicConfig.BootstrapServers, securityOption)
	if err != nil {
		return err
	}

	logger.Debug("Topic deleted", zap.String("topic", topic))

	return nil
}

func (r *Reconciler) reconcileSubscribers(ctx context.Context, ch *messagingv1beta1.KafkaChannel) error {
	after := ch.DeepCopy()
	after.Status.Subscribers = make([]v1.SubscriberStatus, 0)
	for _, s := range ch.Spec.Subscribers {
		if r, _ := r.statusManager.IsReady(ctx, *ch, s); r {
			logging.FromContext(ctx).Debugw("marking subscription", zap.Any("subscription", s))
			after.Status.Subscribers = append(after.Status.Subscribers, v1.SubscriberStatus{
				UID:                s.UID,
				ObservedGeneration: s.Generation,
				Ready:              corev1.ConditionTrue,
			})
		}
	}

	jsonPatch, err := duck.CreatePatch(ch, after)
	if err != nil {
		return fmt.Errorf("creating JSON patch: %w", err)
	}
	// If there is nothing to patch, we are good, just return.
	// Empty patch is [], hence we check for that.
	if len(jsonPatch) == 0 {
		return nil
	}
	patch, err := jsonPatch.MarshalJSON()
	if err != nil {
		return fmt.Errorf("marshaling JSON patch: %w", err)
	}
	patched, err := r.kafkaClientSet.MessagingV1beta1().KafkaChannels(ch.Namespace).Patch(ctx, ch.Name, types.JSONPatchType, patch, metav1.PatchOptions{}, "status")

	if err != nil {
		return fmt.Errorf("Failed patching: %w", err)
	}
	logging.FromContext(ctx).Debugw("Patched resource", zap.Any("patch", patch), zap.Any("patched", patched))
	return nil
}

// TODO: we need something like this
//func (r *Reconciler) topicConfig(logger *zap.Logger, channel *messagingv1beta1.KafkaChannel) (*kafka.TopicConfig, *corev1.ConfigMap, error) {
//
//	logger.Debug("channel config", zap.Any("channel.spec.config", channel.Spec.Config))
//
//	if channel.Spec.Config == nil {
//		tc, err := r.defaultConfig()
//		return tc, nil, err
//	}
//
//	if strings.ToLower(channel.Spec.Config.Kind) != "configmap" { // TODO: is there any constant?
//		return nil, nil, fmt.Errorf("supported config Kind: ConfigMap - got %s", channel.Spec.Config.Kind)
//	}
//
//	namespace := channel.Spec.Config.Namespace
//	if namespace == "" {
//		// Namespace not specified, use channel namespace.
//		namespace = channel.Namespace
//	}
//	cm, err := r.ConfigMapLister.ConfigMaps(namespace).Get(channel.Spec.Config.Name)
//	if err != nil {
//		return nil, nil, fmt.Errorf("failed to get configmap %s/%s: %w", namespace, channel.Spec.Config.Name, err)
//	}
//
//	channelConfig, err := configFromConfigMap(logger, cm)
//	if err != nil {
//		return nil, cm, err
//	}
//
//	return channelConfig, cm, nil
//}

func (r *Reconciler) defaultTopicDetail() sarama.TopicDetail {
	r.KafkaDefaultTopicDetailsLock.RLock()
	defer r.KafkaDefaultTopicDetailsLock.RUnlock()

	// copy the default topic details
	topicDetail := r.KafkaDefaultTopicDetails
	return topicDetail
}

func (r *Reconciler) defaultConfig() (*kafka.TopicConfig, error) {
	bootstrapServers, err := r.getDefaultBootstrapServersOrFail()
	if err != nil {
		return nil, err
	}

	return &kafka.TopicConfig{
		TopicDetail:      r.defaultTopicDetail(),
		BootstrapServers: bootstrapServers,
	}, nil
}

func (r *Reconciler) getChannelResource(ctx context.Context, topic string, channel *messagingv1beta1.KafkaChannel, secret *corev1.Secret, config *kafka.TopicConfig) (*contract.Resource, error) {
	resource := &contract.Resource{
		Uid:    string(channel.UID),
		Topics: []string{topic},
		Ingress: &contract.Ingress{
			IngressType: &contract.Ingress_Path{
				Path: receiver.PathFromObject(channel),
			},
		},
		BootstrapServers: config.GetBootstrapServers(),
	}

	if secret != nil {
		resource.Auth = &contract.Resource_AuthSecret{
			AuthSecret: &contract.Reference{
				Uuid:      string(secret.UID),
				Namespace: secret.Namespace,
				Name:      secret.Name,
				Version:   secret.ResourceVersion,
			},
		}
	}

	egressConfig, err := coreconfig.EgressConfigFromDelivery(ctx, r.Resolver, channel, channel.Spec.Delivery, r.Configs.DefaultBackoffDelayMs)
	if err != nil {
		return nil, err
	}
	resource.EgressConfig = egressConfig

	return resource, nil
}

func (r *Reconciler) ConfigMapUpdated(ctx context.Context) func(configMap *corev1.ConfigMap) {

	logger := logging.FromContext(ctx).Desugar()

	return func(configMap *corev1.ConfigMap) {

		// TODO
		//topicConfig, err := configFromConfigMap(logger, configMap)
		//if err != nil {
		//	return
		//}

		// TODO:
		topicConfig := &kafka.TopicConfig{
			TopicDetail: sarama.TopicDetail{
				NumPartitions:     1,
				ReplicationFactor: 1,
				ReplicaAssignment: nil,
				ConfigEntries:     nil,
			},
			BootstrapServers: []string{"my-cluster-kafka-bootstrap.kafka:9092"},
		}

		logger.Debug("new defaults",
			zap.Any("topicDetail", topicConfig.TopicDetail),
			zap.String("BootstrapServers", topicConfig.GetBootstrapServers()),
		)

		r.SetDefaultTopicDetails(topicConfig.TopicDetail)
		r.SetBootstrapServers(topicConfig.GetBootstrapServers())
	}
}

// SetBootstrapServers change kafka bootstrap channels addresses.
// servers: a comma separated list of channels to connect to.
func (r *Reconciler) SetBootstrapServers(servers string) {
	if servers == "" {
		return
	}

	addrs := kafka.BootstrapServersArray(servers)

	r.bootstrapServersLock.Lock()
	r.bootstrapServers = addrs
	r.bootstrapServersLock.Unlock()
}

func (r *Reconciler) SetDefaultTopicDetails(topicDetail sarama.TopicDetail) {
	r.KafkaDefaultTopicDetailsLock.Lock()
	defer r.KafkaDefaultTopicDetailsLock.Unlock()

	r.KafkaDefaultTopicDetails = topicDetail
}

func (r *Reconciler) getDefaultBootstrapServersOrFail() ([]string, error) {
	r.bootstrapServersLock.RLock()
	defer r.bootstrapServersLock.RUnlock()

	if len(r.bootstrapServers) == 0 {
		return nil, fmt.Errorf("no %s provided", BootstrapServersConfigMapKey)
	}

	return r.bootstrapServers, nil
}
