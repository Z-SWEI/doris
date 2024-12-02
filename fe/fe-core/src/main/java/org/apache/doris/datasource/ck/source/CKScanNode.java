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

package org.apache.doris.datasource.ck.source;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TPlanNode;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CKScanNode extends FileQueryScanNode {
    public CKScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv) {
        super(id, desc, "CKScanNode", StatisticalType.DEFAULT, needCheckColumnPriv);
    }

    @Override
    protected TFileType getLocationType() throws UserException {
        return TFileType.FILE_LOCAL;
    }

    @Override
    protected TFileType getLocationType(String location) throws UserException {
        return TFileType.FILE_LOCAL;
    }

    @Override
    protected TFileFormatType getFileFormatType() throws UserException {
        return TFileFormatType.FORMAT_ARROW;
    }

    @Override
    protected List<String> getPathPartitionKeys() throws UserException {
        return Collections.emptyList();
    }

    @Override
    protected TableIf getTargetTable() throws UserException {
        return desc.getTable();
    }

    @Override
    protected Map<String, String> getLocationProperties() throws UserException {
        return Collections.emptyMap();
    }

    @Override
    public List<Split> getSplits() throws UserException {
        List<Split> splits = new ArrayList<>();
        splits.add(new CKSplit(new Path("/Users/swei/Work/corp/test/arrow/test-id.arrow"), 0, 320, 320, null, null));
        return splits;
    }
}

