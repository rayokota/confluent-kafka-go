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

package encryption

import (
	"fmt"
	"github.com/tink-crypto/tink-go/v2/aead"
	"github.com/tink-crypto/tink-go/v2/core/registry"
	"github.com/tink-crypto/tink-go/v2/subtle"
	"google.golang.org/protobuf/proto"
	"strings"

	agpb "github.com/tink-crypto/tink-go/v2/proto/aes_gcm_go_proto"
	"github.com/tink-crypto/tink-go/v2/tink"
)

const (
	localPrefix = "local-kms://"
)

// localClient represents a client to be used for local testing
type localClient struct {
	keyURIPrefix string
	primitive    tink.AEAD
}

// NewClient returns a new local KMS client
func NewLocalClient(uriPrefix string, secret string) (registry.KMSClient, error) {
	if !strings.HasPrefix(strings.ToLower(uriPrefix), localPrefix) {
		return nil, fmt.Errorf("uriPrefix must start with %s, but got %s", localPrefix, uriPrefix)
	}
	keyBytes, err := subtle.ComputeHKDF("SHA256", []byte(secret), nil, nil, 16)
	if err != nil {
		return nil, err
	}
	aesGCMKey := &agpb.AesGcmKey{Version: 0, KeyValue: keyBytes}
	serializedAESGCMKey, err := proto.Marshal(aesGCMKey)
	if err != nil {
		return nil, err
	}
	/*
		format := &gcmpb.AesGcmKeyFormat{
			KeySize: keySize,
		}
		serializedFormat, err := proto.Marshal(format)
		if err != nil {
			tinkerror.Fail(fmt.Sprintf("failed to marshal key format: %s", err))
		}
		return &tinkpb.KeyTemplate{
			TypeUrl:          aesGCMTypeURL,
			Value:            serializedFormat,
			OutputPrefixType: outputPrefixType,
		}

	*/
	dekTemplate := aead.AES128GCMKeyTemplate()
	primitive, err := registry.Primitive(dekTemplate.TypeUrl, serializedAESGCMKey)
	if err != nil {
		return nil, err
	}
	return &localClient{
		keyURIPrefix: uriPrefix,
		primitive:    primitive.(tink.AEAD),
	}, nil
}

// Supported true if this client does support keyURI
func (c *localClient) Supported(keyURI string) bool {
	return strings.HasPrefix(keyURI, c.keyURIPrefix)
}

// GetAEAD gets an AEAD backend by keyURI.
// keyURI must have the following format: 'local-kms://{key}'.
func (c *localClient) GetAEAD(keyURI string) (tink.AEAD, error) {
	if !c.Supported(keyURI) {
		return nil, fmt.Errorf("keyURI must start with prefix %s, but got %s", c.keyURIPrefix, keyURI)
	}
	return c.primitive, nil
}
