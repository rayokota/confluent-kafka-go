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

package rules

import (
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
)

func init() {
	c := &CelExecutor{}
	serde.RegisterRuleExecutor(c)
}

type CelExecutor struct {
	Config map[string]string
}

// Configure configures the executor
func (c *CelExecutor) Configure(clientConfig *schemaregistry.Config, config map[string]string) error {
	c.Config = config
	return nil
}

func (c *CelExecutor) Type() string {
	return "CEL"
}

func (c *CelExecutor) Transform(ctx serde.RuleContext, msg interface{}) (interface{}, error) {
	return nil, nil
}

func (c *CelExecutor) Close() {
}
