// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.datasource.ck;

import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.SessionContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class CKExternalCatalog extends ExternalCatalog {

    public CKExternalCatalog(long catalogId, String name, String resource, Map<String, String> props, String comment) {
        super(catalogId, name, InitCatalogLog.Type.CK, comment);
        catalogProperty = new CatalogProperty(resource, props);
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        List<String> tbl = new ArrayList<>();
        if ("test_db".equals(dbName)) {
            tbl.add("test_tbl");
        }
        return tbl;
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        return true;
    }

    @Override
    protected void initLocalObjectsImpl() {

    }

    @Override
    public Optional<ExternalDatabase<? extends ExternalTable>> getDb(String dbName) {
        return super.getDb(dbName);
    }

    protected List<String> listDatabaseNames() {
        List<String> db = new ArrayList<>();
        db.add("test_db");
        return db;
    }
}
