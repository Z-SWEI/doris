package org.apache.doris.http;

import okhttp3.Credentials;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okio.BufferedSink;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class LoadActionTest {

    private static final String STREAM_LOAD_URL = "http://localhost:8030/api/%s/%s/_stream_load";
    public static final String DB_NAME = "test";
    public static final String TBL_NAME = "load_task_int_tinyint";
    OkHttpClient client = new OkHttpClient.Builder()
            .addNetworkInterceptor((chain ->
                    chain.proceed(
                        chain.request()
                                .newBuilder()
                                .addHeader("Expect", "100-continue")
                                .addHeader("Connection", "keep-alive")
                                .addHeader("Authorization", Credentials.basic("root", ""))
                                .build()
                    )
            ))
            .build();

    @Test
    public void testStreamLoadString() throws IOException {
        Request request = new Request.Builder()
                .url(String.format(STREAM_LOAD_URL, DB_NAME, TBL_NAME))
                .put(RequestBody.create(MediaType.parse("application/x-www-form-urlencoded"), "1,a,0.1,20240101"))
                .header("Expect", "100-continue")
                .header("column_separator", ",")
                .build();
        try (Response response = client.newCall(request).execute()) {
            System.out.println(response.body().string());
        }
    }

    @Test
    public void testStreamLoadStream() throws IOException {

        RequestBody requestBody = new RequestBody() {
            @Override
            public MediaType contentType() {
                return MediaType.parse("text/plain");
            }

            @Override
            public void writeTo(BufferedSink bufferedSink) throws IOException {
                try (BufferAllocator bufferAllocator = new RootAllocator(160000)) {
//                    arrowBuf.close();
                    try(
                            IntVector intVector = new IntVector("id", bufferAllocator);
                            TinyIntVector tinyIntVector = new TinyIntVector("c_tinyint", bufferAllocator);
                            VarCharVector nameVector = new VarCharVector("name", bufferAllocator);
                            Float8Vector float8Vector = new Float8Vector("s", bufferAllocator);
                            VarCharVector pDateVector = new VarCharVector("p_date", bufferAllocator);
                    ) {
                        //VectorSchemaRoot root = VectorSchemaRoot.of(intVector, nameVector, float8Vector, pDateVector);
                        VectorSchemaRoot root = VectorSchemaRoot.of(intVector,tinyIntVector);
                        root.allocateNew();
//                        intVector.allocateNew();
//                        nameVector.allocateNew();
//                        float8Vector.allocateNew(1);
//                        pDateVector.allocateNew(1);
                        intVector.set(0, 1);
                        tinyIntVector.set(0, 1);
                        //nameVector.set(0, "b".getBytes());
                        //float8Vector.set(0, 0.1);
                        //pDateVector.set(0, "20240101".getBytes());

//                        List<Field> fields = Arrays.asList(intVector.getField(), nameVector.getField(), float8Vector.getField(), pDateVector.getField());
//                        List<FieldVector> vectors = Arrays.asList(intVector, nameVector, float8Vector, pDateVector);
                        root.setRowCount(1);


                        for (int i = 0; i < 2; i++) {
                            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, bufferedSink)) {
                                root.setRowCount(1);
                                writer.start();
                                writer.writeBatch();
                                writer.end();
                                bufferedSink.flush();
                            }
                        }


                        // System.out.println(root.contentToTSVString());
                    }
                }
            }
        };
        Request request = new Request.Builder()
                .url(String.format(STREAM_LOAD_URL, DB_NAME, TBL_NAME))
                .put(requestBody)
                .header("Expect", "100-continue")
                .header("Connection", "keep-alive")
                .header("format", "arrow")
                .build();
        try (Response response = client.newCall(request).execute()) {
            System.out.println(response.body().string());
        }
    }
}
