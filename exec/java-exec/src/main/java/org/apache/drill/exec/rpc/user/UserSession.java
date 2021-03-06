/**
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
package org.apache.drill.exec.rpc.user;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import net.hydromatic.optiq.SchemaPlus;

import org.apache.drill.exec.proto.UserBitShared.UserCredentials;
import org.apache.drill.exec.proto.UserProtos.Property;
import org.apache.drill.exec.proto.UserProtos.UserProperties;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.SessionOptionManager;

import com.google.common.collect.Maps;

public class UserSession {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UserSession.class);

  public static final String SCHEMA = "schema";

  private DrillUser user;
  private boolean enableExchanges = true;
  private UserCredentials credentials;
  private Map<String, String> properties;
  private OptionManager options;
  private ViewStore viewStore;

  public UserSession(OptionManager systemOptions, UserCredentials credentials, UserProperties properties) throws IOException{
    this.credentials = credentials;
    this.options = new SessionOptionManager(systemOptions);
    this.properties = Maps.newHashMap();
    this.viewStore = new ViewStore();

    if (properties == null) return;
    for (int i=0; i<properties.getPropertiesCount(); i++) {
      Property prop = properties.getProperties(i);
      this.properties.put(prop.getKey(), prop.getValue());
    }
  }

  public OptionManager getOptions(){
    return options;
  }

  public DrillUser getUser(){
    return user;
  }

  public ViewStore getViewStore() { return viewStore; }

  /**
   * Update the schema path for the session.
   * @param fullPath The desired path to set to.
   * @param schema The root schema to find this path within.
   * @return true if the path was set successfully.  false if this path was unavailable.
   */
  public boolean setDefaultSchemaPath(String fullPath, SchemaPlus schema){
    SchemaPlus newDefault = findSchema(schema, fullPath);
    if(newDefault == null) return false;
    setProp(SCHEMA, fullPath);
    return true;
  }

  /**
   * Get default schema from current default schema path and given schema tree.
   * @param rootSchema
   * @return A {@link net.hydromatic.optiq.SchemaPlus} object.
   */
  public SchemaPlus getDefaultSchema(SchemaPlus rootSchema){
    return findSchema(rootSchema, getProp(SCHEMA));
  }

  public boolean setSessionOption(String name, String value){
    return true;
  }

  private String getProp(String key) {
    return properties.get(key) != null ? properties.get(key) : "";
  }

  private void setProp(String key, String value) {
    properties.put(key, value);
  }

  private SchemaPlus findSchema(SchemaPlus rootSchema, String schemaPath) {
    String[] paths = schemaPath.split("\\.");
    SchemaPlus schema = rootSchema;
    for(String p : paths){
      schema = schema.getSubSchema(p);
      if(schema == null) break;
    }
    return schema;
  }

}
