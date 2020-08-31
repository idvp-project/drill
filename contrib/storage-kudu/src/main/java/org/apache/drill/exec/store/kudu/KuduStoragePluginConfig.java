/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.kudu;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.drill.common.logical.StoragePluginConfigBase;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(KuduStoragePluginConfig.NAME)
@JsonIgnoreProperties(ignoreUnknown = true)
public class KuduStoragePluginConfig extends StoragePluginConfigBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(KuduStoragePluginConfig.class);

  public static final String NAME = "kudu";

  private final String masterAddresses;
  private final boolean faultTolerant;
  private final Integer scanTimeout;

  @JsonCreator
  public KuduStoragePluginConfig(@JsonProperty("masterAddresses") String masterAddresses,
                                 @JsonProperty("faultTolerant") Boolean faultTolerant,
                                 @JsonProperty("scanTimeout") Integer scanTimeout) {
    this.masterAddresses = masterAddresses;
    this.faultTolerant = !Boolean.FALSE.equals(faultTolerant); // true by default
    this.scanTimeout = scanTimeout;
  }

  public String getMasterAddresses() {
    return masterAddresses;
  }

  public boolean isFaultTolerant() {
    return faultTolerant;
  }

  public Integer getScanTimeout() {
    return scanTimeout;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((masterAddresses == null) ? 0 : masterAddresses.hashCode());
    result = prime * result + Boolean.hashCode(faultTolerant);
    result = prime * result + ((scanTimeout == null) ? 0 : scanTimeout.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    KuduStoragePluginConfig other = (KuduStoragePluginConfig) obj;
    if (masterAddresses == null) {
      if (other.masterAddresses != null) {
        return false;
      }
    } else if (!masterAddresses.equals(other.masterAddresses)) {
      return false;
    }

    if (scanTimeout == null) {
      if (other.scanTimeout != null) {
        return false;
      }
    } else if (!scanTimeout.equals(other.scanTimeout)) {
      return false;
    }

    return faultTolerant == other.faultTolerant;
  }



}
