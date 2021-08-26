/*
Copyright 2021 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Code generated by lister-gen. DO NOT EDIT.

package v1alpha1

import (
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
	v1alpha1 "knative.dev/eventing-kafka/pkg/apis/kafka/v1alpha1"
)

// ResetOffsetLister helps list ResetOffsets.
// All objects returned here must be treated as read-only.
type ResetOffsetLister interface {
	// List lists all ResetOffsets in the indexer.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*v1alpha1.ResetOffset, err error)
	// ResetOffsets returns an object that can list and get ResetOffsets.
	ResetOffsets(namespace string) ResetOffsetNamespaceLister
	ResetOffsetListerExpansion
}

// resetOffsetLister implements the ResetOffsetLister interface.
type resetOffsetLister struct {
	indexer cache.Indexer
}

// NewResetOffsetLister returns a new ResetOffsetLister.
func NewResetOffsetLister(indexer cache.Indexer) ResetOffsetLister {
	return &resetOffsetLister{indexer: indexer}
}

// List lists all ResetOffsets in the indexer.
func (s *resetOffsetLister) List(selector labels.Selector) (ret []*v1alpha1.ResetOffset, err error) {
	err = cache.ListAll(s.indexer, selector, func(m interface{}) {
		ret = append(ret, m.(*v1alpha1.ResetOffset))
	})
	return ret, err
}

// ResetOffsets returns an object that can list and get ResetOffsets.
func (s *resetOffsetLister) ResetOffsets(namespace string) ResetOffsetNamespaceLister {
	return resetOffsetNamespaceLister{indexer: s.indexer, namespace: namespace}
}

// ResetOffsetNamespaceLister helps list and get ResetOffsets.
// All objects returned here must be treated as read-only.
type ResetOffsetNamespaceLister interface {
	// List lists all ResetOffsets in the indexer for a given namespace.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*v1alpha1.ResetOffset, err error)
	// Get retrieves the ResetOffset from the indexer for a given namespace and name.
	// Objects returned here must be treated as read-only.
	Get(name string) (*v1alpha1.ResetOffset, error)
	ResetOffsetNamespaceListerExpansion
}

// resetOffsetNamespaceLister implements the ResetOffsetNamespaceLister
// interface.
type resetOffsetNamespaceLister struct {
	indexer   cache.Indexer
	namespace string
}

// List lists all ResetOffsets in the indexer for a given namespace.
func (s resetOffsetNamespaceLister) List(selector labels.Selector) (ret []*v1alpha1.ResetOffset, err error) {
	err = cache.ListAllByNamespace(s.indexer, s.namespace, selector, func(m interface{}) {
		ret = append(ret, m.(*v1alpha1.ResetOffset))
	})
	return ret, err
}

// Get retrieves the ResetOffset from the indexer for a given namespace and name.
func (s resetOffsetNamespaceLister) Get(name string) (*v1alpha1.ResetOffset, error) {
	obj, exists, err := s.indexer.GetByKey(s.namespace + "/" + name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.NewNotFound(v1alpha1.Resource("resetoffset"), name)
	}
	return obj.(*v1alpha1.ResetOffset), nil
}
