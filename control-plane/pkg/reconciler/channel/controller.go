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
	"errors"
	"fmt"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	messagingv1beta1 "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/messaging/v1beta1"
	eventingduckv1 "knative.dev/eventing/pkg/apis/duck/v1"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/resolver"
	"knative.dev/pkg/tracker"

	kafkaChannelClient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/client"
	kafkachannelinformer "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/informers/messaging/v1beta1/kafkachannel"
	kafkachannelreconciler "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/reconciler/messaging/v1beta1/kafkachannel"
	configmapinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/configmap"
	endpointsinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/endpoints"
	podinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/pod"
	secretinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/secret"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
)

// TODO use or delete the constants here
const (
	DefaultTopicNumPartitionConfigMapKey      = "default.topic.partitions"
	DefaultTopicReplicationFactorConfigMapKey = "default.topic.replication.factor"
	BootstrapServersConfigMapKey              = "bootstrap.servers"

	DefaultNumPartitions     = 10
	DefaultReplicationFactor = 1
)

func NewController(ctx context.Context, watcher configmap.Watcher, configs *Configs) *controller.Impl {

	// TODO: are these conditions good?
	messagingv1beta1.RegisterAlternateKafkaChannelConditionSet(base.ConditionSet)

	configmapInformer := configmapinformer.Get(ctx)
	endpointsInformer := endpointsinformer.Get(ctx)

	reconciler := &Reconciler{
		Reconciler: &base.Reconciler{
			KubeClient:                  kubeclient.Get(ctx),
			PodLister:                   podinformer.Get(ctx).Lister(),
			SecretLister:                secretinformer.Get(ctx).Lister(),
			DataPlaneConfigMapNamespace: configs.DataPlaneConfigMapNamespace,
			DataPlaneConfigMapName:      configs.DataPlaneConfigMapName,
			DataPlaneConfigFormat:       configs.DataPlaneConfigFormat,
			SystemNamespace:             configs.SystemNamespace,
			DispatcherLabel:             base.ChannelDispatcherLabel,
			ReceiverLabel:               base.ChannelReceiverLabel,
		},
		ClusterAdmin: sarama.NewClusterAdmin,
		KafkaDefaultTopicDetails: sarama.TopicDetail{
			NumPartitions:     DefaultNumPartitions,
			ReplicationFactor: DefaultReplicationFactor,
		},
		ConfigMapLister: configmapInformer.Lister(),
		endpointsLister: endpointsInformer.Lister(),
		kafkaClientSet:  kafkaChannelClient.Get(ctx),
		Configs:         configs,
	}

	logger := logging.FromContext(ctx)

	_, err := reconciler.GetOrCreateDataPlaneConfigMap(ctx)
	if err != nil {
		logger.Fatal("Failed to get or create data plane config map",
			zap.String("configmap", configs.DataPlaneConfigMapAsString()),
			zap.Error(err),
		)
	}

	if configs.BootstrapServers != "" {
		reconciler.SetBootstrapServers(configs.BootstrapServers)
	}

	impl := kafkachannelreconciler.NewImpl(ctx, reconciler)

	statusProber := NewProber(
		logger.Named("status-manager"),
		NewProbeTargetLister(logger, endpointsInformer.Lister()),
		func(c messagingv1beta1.KafkaChannel, s eventingduckv1.SubscriberSpec) {
			logger.Debugf("Ready callback triggered for channel: %s/%s subscription: %s", c.Namespace, c.Name, string(s.UID))
			impl.EnqueueKey(types.NamespacedName{Namespace: c.Namespace, Name: c.Name})
		},
	)
	reconciler.statusManager = statusProber
	statusProber.Start(ctx.Done())

	reconciler.Resolver = resolver.NewURIResolver(ctx, impl.EnqueueKey)

	channelInformer := kafkachannelinformer.Get(ctx)

	logger.Info("Register event handlers")

	// TODO: any filters necessary?
	channelInformer.Informer().AddEventHandler(controller.HandleAll(impl.Enqueue))

	globalResync := func(_ interface{}) {
		impl.GlobalResync(channelInformer.Informer())
	}

	configmapInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: controller.FilterWithNameAndNamespace(configs.DataPlaneConfigMapNamespace, configs.DataPlaneConfigMapName),
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				globalResync(obj)
			},
			DeleteFunc: func(obj interface{}) {
				globalResync(obj)
			},
		},
	})

	reconciler.SecretTracker = tracker.New(impl.EnqueueKey, controller.GetTrackerLease(ctx))
	secretinformer.Get(ctx).Informer().AddEventHandler(controller.HandleAll(reconciler.SecretTracker.OnChanged))

	reconciler.ConfigMapTracker = tracker.New(impl.EnqueueKey, controller.GetTrackerLease(ctx))
	configmapinformer.Get(ctx).Informer().AddEventHandler(controller.HandleAll(
		// Call the tracker's OnChanged method, but we've seen the objects
		// coming through this path missing TypeMeta, so ensure it is properly
		// populated.
		controller.EnsureTypeMeta(
			reconciler.ConfigMapTracker.OnChanged,
			corev1.SchemeGroupVersion.WithKind("ConfigMap"),
		),
	))

	// TODO: any filters necessary?
	channelInformer.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			DeleteFunc: reconciler.OnDeleteObserver,
		},
	)

	cm, err := reconciler.KubeClient.CoreV1().ConfigMaps(configs.SystemNamespace).Get(ctx, configs.GeneralConfigMapName, metav1.GetOptions{})
	if err != nil {
		panic(fmt.Errorf("failed to get config map %s/%s: %w", configs.SystemNamespace, configs.GeneralConfigMapName, err))
	}

	reconciler.ConfigMapUpdated(ctx)(cm)

	watcher.Watch(configs.GeneralConfigMapName, reconciler.ConfigMapUpdated(ctx))

	return impl
}

func ValidateDefaultBackoffDelayMs(env config.Env) error {
	if env.DefaultBackoffDelayMs == 0 {
		return errors.New("default backoff delay cannot be 0")
	}
	return nil
}
