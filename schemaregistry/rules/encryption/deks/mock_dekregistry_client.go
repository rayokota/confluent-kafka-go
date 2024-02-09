/**
 * Copyright 2024 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package deks

import (
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/internal"
	"net/url"
	"sync"
)

/* HTTP(S) DEK Registry Client and caches */
type mockclient struct {
	sync.Mutex
	url          *url.URL
	kekCache     map[KekID]Kek
	kekCacheLock sync.RWMutex
	dekCache     map[DekID]Dek
	dekCacheLock sync.RWMutex
}

var _ Client = new(mockclient)

// RegisterKek registers kek
func (c *mockclient) RegisterKek(name string, kmsType string, kmsKeyID string, kmsProps map[string]string, doc string, shared bool) (kek Kek, err error) {
	cacheKey := KekID{
		Name:    name,
		Deleted: false,
	}
	c.kekCacheLock.RLock()
	kek, ok := c.kekCache[cacheKey]
	c.kekCacheLock.RUnlock()
	if ok {
		return kek, nil
	}

	kek = Kek{
		Name:     name,
		KmsType:  kmsType,
		KmsKeyID: kmsKeyID,
		KmsProps: kmsProps,
		Doc:      doc,
		Shared:   shared,
	}
	c.kekCacheLock.Lock()
	c.kekCache[cacheKey] = kek
	c.kekCacheLock.Unlock()
	return kek, nil
}

// GetKek returns the kek identified by name
// Returns kek object on success
func (c *mockclient) GetKek(name string, deleted bool) (kek Kek, err error) {
	cacheKey := KekID{
		Name:    name,
		Deleted: false,
	}
	c.kekCacheLock.RLock()
	kek, ok := c.kekCache[cacheKey]
	c.kekCacheLock.RUnlock()
	if ok {
		if !kek.Deleted || deleted {
			return kek, nil
		}
	}
	posErr := internal.RestError{
		Code:    404,
		Message: "Key Not Found",
	}
	return Kek{}, &posErr
}

// RegisterDek registers dek
func (c *mockclient) RegisterDek(kekName string, subject string, algorithm string, encryptedKeyMaterial string) (dek Dek, err error) {
	return c.RegisterDekVersion(kekName, subject, 1, algorithm, encryptedKeyMaterial)
}

// GetDek returns the dek
// Returns dek object on success
func (c *mockclient) GetDek(kekName string, subject string, algorithm string, deleted bool) (dek Dek, err error) {
	return c.GetDekVersion(kekName, subject, 1, algorithm, deleted)
}

// RegisterDekVersion registers versioned dek
func (c *mockclient) RegisterDekVersion(kekName string, subject string, version int, algorithm string, encryptedKeyMaterial string) (dek Dek, err error) {
	cacheKey := DekID{
		KekName:   kekName,
		Subject:   subject,
		Version:   version,
		Algorithm: algorithm,
		Deleted:   false,
	}
	c.dekCacheLock.RLock()
	dek, ok := c.dekCache[cacheKey]
	c.dekCacheLock.RUnlock()
	if ok {
		return dek, nil
	}

	dek = Dek{
		KekName:              kekName,
		Subject:              subject,
		Version:              version,
		Algorithm:            algorithm,
		EncryptedKeyMaterial: encryptedKeyMaterial,
	}
	c.dekCacheLock.Lock()
	c.dekCache[cacheKey] = dek
	c.dekCacheLock.Unlock()
	return dek, nil
}

// GetDekVersion returns the versioned dek
// Returns dek object on success
func (c *mockclient) GetDekVersion(kekName string, subject string, version int, algorithm string, deleted bool) (dek Dek, err error) {
	cacheKey := DekID{
		KekName:   kekName,
		Subject:   subject,
		Version:   version,
		Algorithm: algorithm,
		Deleted:   false,
	}
	c.dekCacheLock.RLock()
	dek, ok := c.dekCache[cacheKey]
	c.dekCacheLock.RUnlock()
	if ok {
		if !dek.Deleted || deleted {
			return dek, nil
		}
	}
	posErr := internal.RestError{
		Code:    404,
		Message: "Key Not Found",
	}
	return Dek{}, &posErr
}
