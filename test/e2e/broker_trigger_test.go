//go:build e2e
// +build e2e

/*
 * Copyright 2020 The Knative Authors
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

package e2e

import (
	"context"
	"fmt"
	"knative.dev/eventing-kafka/test/e2e/helpers"
	"testing"
	"time"

	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/client-go/util/retry"

	. "github.com/cloudevents/sdk-go/v2/test"
	"github.com/stretchr/testify/assert"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	testlib "knative.dev/eventing/test/lib"
	"knative.dev/eventing/test/lib/recordevents"
	"knative.dev/eventing/test/lib/resources"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/broker"
	kafkatest "knative.dev/eventing-kafka-broker/test/pkg/kafka"
	testingpkg "knative.dev/eventing-kafka-broker/test/pkg/testing"
)

func TestBrokerTrigger(t *testing.T) {
	testingpkg.RunMultiple(t, func(t *testing.T) {

		ctx := context.Background()

		client := testlib.Setup(t, true)
		defer testlib.TearDown(client)

		const (
			senderName  = "sender"
			triggerName = "trigger"
			subscriber  = "subscriber"

			defaultNumPartitions     = 10
			defaultReplicationFactor = 3
			verifierName             = "num-partitions-replication-factor-verifier"

			eventType       = "type1"
			eventSource     = "source1"
			eventBody       = `{"msg":"e2e-eventtransformation-body"}`
			extension1      = "ext1"
			valueExtension1 = "value1"
		)

		nonMatchingEventId := uuid.New().String()
		eventId := uuid.New().String()

		br := client.CreateBrokerOrFail(
			brokerName,
			resources.WithBrokerClassForBroker(kafka.BrokerClass),
		)

		eventTracker, _ := recordevents.StartEventRecordOrFail(ctx, client, subscriber)

		client.CreateTriggerOrFail(
			triggerName,
			resources.WithBroker(brokerName),
			resources.WithSubscriberServiceRefForTrigger(subscriber),
			func(trigger *eventing.Trigger) {
				trigger.Spec.Filter = &eventing.TriggerFilter{
					Attributes: map[string]string{
						"source":   eventSource,
						extension1: valueExtension1,
						"type":     "",
					},
				}
			},
		)

		client.WaitForAllTestResourcesReadyOrFail(ctx)

		t.Logf("Sending events to %s/%s", client.Namespace, brokerName)

		nonMatchingEvent := cloudevents.NewEvent()
		nonMatchingEvent.SetID(eventId)
		nonMatchingEvent.SetType(eventType)
		nonMatchingEvent.SetSource(eventSource)
		nonMatchingEvent.SetExtension(extension1, valueExtension1+"a")
		if err := nonMatchingEvent.SetData(cloudevents.ApplicationJSON, []byte(eventBody)); err != nil {
			t.Fatalf("Cannot set the payload of the event: %s", err.Error())
		}

		client.SendEventToAddressable(
			ctx,
			senderName+"non-matching",
			brokerName,
			testlib.BrokerTypeMeta,
			nonMatchingEvent,
		)

		eventToSend := cloudevents.NewEvent()
		eventToSend.SetID(eventId)
		eventToSend.SetType(eventType)
		eventToSend.SetSource(eventSource)
		eventToSend.SetExtension(extension1, valueExtension1)
		if err := eventToSend.SetData(cloudevents.ApplicationJSON, []byte(eventBody)); err != nil {
			t.Fatalf("Cannot set the payload of the event: %s", err.Error())
		}

		client.SendEventToAddressable(
			ctx,
			senderName+"matching",
			brokerName,
			testlib.BrokerTypeMeta,
			eventToSend,
		)

		eventTracker.AssertAtLeast(1, recordevents.MatchEvent(
			HasId(eventId),
			HasSource(eventSource),
			HasType(eventType),
			HasData([]byte(eventBody)),
		))

		eventTracker.AssertNot(recordevents.MatchEvent(
			HasId(nonMatchingEventId),
		))

		config := &kafkatest.Config{
			BootstrapServers:  testingpkg.BootstrapServersPlaintext,
			ReplicationFactor: defaultReplicationFactor,
			NumPartitions:     defaultNumPartitions,
			Topic:             kafka.BrokerTopic(broker.TopicPrefix, br),
		}

		err := kafkatest.VerifyNumPartitionAndReplicationFactor(
			client.Kube,
			client.Tracker,
			client.Namespace,
			verifierName,
			config,
		)
		if err != nil {
			t.Errorf("failed to verify num partitions and replication factors: %v", err)
		}
	})
}

func TestBrokerWithConfig(t *testing.T) {
	testingpkg.RunMultiple(t, func(t *testing.T) {

		ctx := context.Background()

		client := testlib.Setup(t, true)
		defer testlib.TearDown(client)

		const (
			configMapName = "br-config"
			senderName    = "sender"
			triggerName   = "trigger"
			subscriber    = "subscriber"

			numPartitions     = 20
			replicationFactor = 1
			verifierName      = "num-partitions-replication-factor-verifier"

			eventType       = "type1"
			eventSource     = "source1"
			eventBody       = `{"msg":"e2e-body"}`
			extension1      = "ext1"
			valueExtension1 = "value1"
		)

		eventId := uuid.New().String()

		cm := client.CreateConfigMapOrFail(configMapName, client.Namespace, map[string]string{
			kafka.DefaultTopicNumPartitionConfigMapKey:      fmt.Sprintf("%d", numPartitions),
			kafka.DefaultTopicReplicationFactorConfigMapKey: fmt.Sprintf("%d", replicationFactor),
			kafka.BootstrapServersConfigMapKey:              testingpkg.BootstrapServersPlaintext,
		})

		br := client.CreateBrokerOrFail(
			brokerName,
			resources.WithBrokerClassForBroker(kafka.BrokerClass),
			resources.WithConfigForBroker(&duckv1.KReference{
				Kind:       "ConfigMap",
				Namespace:  cm.Namespace,
				Name:       cm.Name,
				APIVersion: "v1",
			}),
		)

		eventTracker, _ := recordevents.StartEventRecordOrFail(ctx, client, subscriber)

		client.CreateTriggerOrFail(
			triggerName,
			resources.WithBroker(brokerName),
			resources.WithSubscriberServiceRefForTrigger(subscriber),
			func(trigger *eventing.Trigger) {
				trigger.Spec.Filter = &eventing.TriggerFilter{
					Attributes: map[string]string{
						"source":   eventSource,
						extension1: valueExtension1,
						"type":     "",
					},
				}
			},
		)

		client.WaitForAllTestResourcesReadyOrFail(ctx)

		t.Logf("Sending events to %s/%s", client.Namespace, brokerName)

		eventToSend := cloudevents.NewEvent()
		eventToSend.SetID(eventId)
		eventToSend.SetType(eventType)
		eventToSend.SetSource(eventSource)
		eventToSend.SetExtension(extension1, valueExtension1)
		if err := eventToSend.SetData(cloudevents.ApplicationJSON, []byte(eventBody)); err != nil {
			t.Fatalf("Cannot set the payload of the event: %s", err.Error())
		}

		client.SendEventToAddressable(
			ctx,
			senderName+"matching",
			brokerName,
			testlib.BrokerTypeMeta,
			eventToSend,
		)

		eventTracker.AssertAtLeast(1, recordevents.MatchEvent(
			HasId(eventId),
			HasSource(eventSource),
			HasType(eventType),
			HasData([]byte(eventBody)),
		))

		t.Logf("Verify num partitions and replication factor")

		config := &kafkatest.Config{
			BootstrapServers:  testingpkg.BootstrapServersPlaintext,
			ReplicationFactor: replicationFactor,
			NumPartitions:     numPartitions,
			Topic:             kafka.BrokerTopic(broker.TopicPrefix, br),
		}

		err := kafkatest.VerifyNumPartitionAndReplicationFactor(
			client.Kube,
			client.Tracker,
			client.Namespace,
			verifierName,
			config,
		)
		if err != nil {
			t.Errorf("failed to verify num partitions and replication factors: %v", err)
		}
	})
}

func TestBrokerExternalTopicTrigger(t *testing.T) {
	testingpkg.RunMultiple(t, func(t *testing.T) {

		ctx := context.Background()

		client := testlib.Setup(t, true)
		defer testlib.TearDown(client)

		const (
			kafkaClusterName      = "my-cluster"
			kafkaClusterNamespace = "kafka"
			kafkaTopicSuffix      = "my-topic"

			senderName  = "sender"
			triggerName = "trigger"
			subscriber  = "subscriber"

			eventType       = "type1"
			eventSource     = "source1"
			eventBody       = `{"msg":"e2e-eventtransformation-body"}`
			extension1      = "ext1"
			valueExtension1 = "value1"
		)

		testTopicName := client.Namespace + "-" + kafkaTopicSuffix
		helpers.MustCreateKafkaUserForTopic(client, kafkaClusterName, kafkaClusterNamespace, "tls", testTopicName, testTopicName)
		helpers.MustCreateTopic(client, kafkaClusterName, kafkaClusterNamespace, testTopicName, 10)

		nonMatchingEventId := uuid.New().String()
		eventId := uuid.New().String()

		cm := client.CreateConfigMapOrFail(testTopicName, client.Namespace, map[string]string{
			"default.topic.replication.factor": "2",
			"default.topic.partitions":         "2",
			kafka.BootstrapServersConfigMapKey: testingpkg.BootstrapServersSsl,
			"auth.secret.ref.name":             testTopicName,
		})

		client.CreateBrokerOrFail(
			brokerName,
			resources.WithBrokerClassForBroker(kafka.BrokerClass),
			resources.WithCustomAnnotationForBroker(broker.ExternalTopicAnnotation, testTopicName),
			resources.WithConfigForBroker(&duckv1.KReference{
				Kind:       "ConfigMap",
				Namespace:  cm.Namespace,
				Name:       cm.Name,
				APIVersion: "v1",
			}),
		)

		// secret doesn't exist, so broker won't become ready.
		time.Sleep(time.Second * 30)
		br, err := client.Eventing.EventingV1().Brokers(client.Namespace).Get(ctx, brokerName, metav1.GetOptions{})
		assert.Nil(t, err)
		assert.False(t, br.IsReady(), "secret %s/%s doesn't exist, so broker must no be ready", client.Namespace, testTopicName)

		secretData := sslWithUsername(t, client, testTopicName)

		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: client.Namespace,
				Name:      testTopicName,
			},
			Data: secretData,
		}

		_, err = client.Kube.CoreV1().Secrets(client.Namespace).Create(ctx, secret, metav1.CreateOptions{})
		assert.Nil(t, err)

		// Trigger a reconciliation by updating the referenced ConfigMap in broker.spec.config.
		err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
			config, err := client.Kube.CoreV1().ConfigMaps(client.Namespace).Get(ctx, cm.Name, metav1.GetOptions{})
			if err != nil {
				return nil
			}

			if config.Labels == nil {
				config.Labels = make(map[string]string, 1)
			}
			config.Labels["test.eventing.knative.dev/updated"] = names.SimpleNameGenerator.GenerateName("now")

			_, err = client.Kube.CoreV1().ConfigMaps(client.Namespace).Update(ctx, config, metav1.UpdateOptions{})
			return err
		})
		assert.Nil(t, err)

		client.WaitForResourceReadyOrFail(brokerName, testlib.BrokerTypeMeta)


		eventTracker, _ := recordevents.StartEventRecordOrFail(ctx, client, subscriber)

		client.CreateTriggerOrFail(
			triggerName,
			resources.WithBroker(brokerName),
			resources.WithSubscriberServiceRefForTrigger(subscriber),
			func(trigger *eventing.Trigger) {
				trigger.Spec.Filter = &eventing.TriggerFilter{
					Attributes: map[string]string{
						"source":   eventSource,
						extension1: valueExtension1,
						"type":     "",
					},
				}
			},
		)

		client.WaitForAllTestResourcesReadyOrFail(ctx)

		t.Logf("Sending events to %s/%s", client.Namespace, brokerName)

		nonMatchingEvent := cloudevents.NewEvent()
		nonMatchingEvent.SetID(eventId)
		nonMatchingEvent.SetType(eventType)
		nonMatchingEvent.SetSource(eventSource)
		nonMatchingEvent.SetExtension(extension1, valueExtension1+"a")
		if err := nonMatchingEvent.SetData(cloudevents.ApplicationJSON, []byte(eventBody)); err != nil {
			t.Fatalf("Cannot set the payload of the event: %s", err.Error())
		}

		client.SendEventToAddressable(
			ctx,
			senderName+"non-matching",
			brokerName,
			testlib.BrokerTypeMeta,
			nonMatchingEvent,
		)

		eventToSend := cloudevents.NewEvent()
		eventToSend.SetID(eventId)
		eventToSend.SetType(eventType)
		eventToSend.SetSource(eventSource)
		eventToSend.SetExtension(extension1, valueExtension1)
		if err := eventToSend.SetData(cloudevents.ApplicationJSON, []byte(eventBody)); err != nil {
			t.Fatalf("Cannot set the payload of the event: %s", err.Error())
		}

		client.SendEventToAddressable(
			ctx,
			senderName+"matching",
			brokerName,
			testlib.BrokerTypeMeta,
			eventToSend,
		)

		eventTracker.AssertAtLeast(1, recordevents.MatchEvent(
			HasId(eventId),
			HasSource(eventSource),
			HasType(eventType),
			HasData([]byte(eventBody)),
		))

		eventTracker.AssertNot(recordevents.MatchEvent(
			HasId(nonMatchingEventId),
		))
	})
}


func sslWithUsername(t *testing.T, client *testlib.Client, userName string) map[string][]byte {
	caSecret, err := client.Kube.CoreV1().Secrets("kafka").Get(context.Background(), "my-cluster-cluster-ca-cert", metav1.GetOptions{})
	assert.Nil(t, err)

	tlsUserSecret, err := client.Kube.CoreV1().Secrets("kafka").Get(context.Background(), userName, metav1.GetOptions{})
	assert.Nil(t, err)

	return map[string][]byte{
		"protocol": []byte("SSL"),
		"ca.crt":   caSecret.Data["ca.crt"],
		"user.crt": tlsUserSecret.Data["user.crt"],
		"user.key": tlsUserSecret.Data["user.key"],
	}
}
