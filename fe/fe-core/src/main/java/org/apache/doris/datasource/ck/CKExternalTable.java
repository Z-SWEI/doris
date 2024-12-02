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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.thrift.TEsTable;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TOlapTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

public class CKExternalTable extends ExternalTable {
    public CKExternalTable(long id, String name, String dbName, CKExternalCatalog catalog) {
        super(id, name, catalog, dbName, TableType.CK_EXTERNAL_TABLE);
    }

    @Override
    public Optional<SchemaCacheValue> initSchema() {
        List<Column> cols = new ArrayList<>();
        cols.add(new Column("id", Type.INT));
        return Optional.of(new SchemaCacheValue(cols));
    }

    @Override
    public TTableDescriptor toThrift() {
        List<Column> schema = getFullSchema();
        TEsTable tOlapTable = new TEsTable();
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.ES_TABLE, schema.size(),
                0, getName(), dbName);
        tTableDescriptor.setEsTable(tOlapTable);
        return tTableDescriptor;
    }

}
