// package com.amex.sink;

// import com.amex.lumi.beam.model.IngestionExecution;
// import com.amex.lumi.beam.model.IngestionExecutionStatus;
// import com.amex.lumi.beam.sink.WriteIngestionExecutionToPostgresFn;

// import org.junit.jupiter.api.Test;

// import java.sql.Connection;
// import java.sql.DriverManager;
// import java.sql.PreparedStatement;
// import java.sql.ResultSet;
// import java.time.Instant;

// import static org.junit.jupiter.api.Assertions.assertEquals;
// import static org.junit.jupiter.api.Assertions.assertNull;

// class WriteIngestionExecutionToPostgresFnTest {

//     private static final String JDBC_URL =
//             "jdbc:postgresql://localhost:5432/warehouse";

//     private static final String USERNAME =
//             "airflow";

//     private static final String PASSWORD =
//             "airflow";

//     @Test
//     void shouldWriteSuccessfulExecutionToPostgres()
//             throws Exception {

//         String executionId =
//                 "760e8400-e29b-41d4-a716-446655440000";

//         deleteExecution(executionId);

//         IngestionExecution execution =
//                 new IngestionExecution(
//                         executionId,
//                         "employees.json",
//                         IngestionExecutionStatus.SUCCESS,
//                         10L,
//                         10L,
//                         Instant.parse(
//                                 "2026-09-15T12:00:00Z"
//                         ),
//                         Instant.parse(
//                                 "2026-09-15T12:01:00Z"
//                         ),
//                         null
//                 );

//         WriteIngestionExecutionToPostgresFn writer =
//                 new WriteIngestionExecutionToPostgresFn(
//                         JDBC_URL,
//                         USERNAME,
//                         PASSWORD
//                 );

//         writer.setup();

//         try {
//             writer.processElement(
//                     new TestProcessContext(execution)
//             );
//         } finally {
//             writer.teardown();
//         }

//         try (Connection connection =
//                      DriverManager.getConnection(
//                              JDBC_URL,
//                              USERNAME,
//                              PASSWORD
//                      );
//              PreparedStatement statement =
//                      connection.prepareStatement(
//                              """
//                              SELECT
//                                  source_file,
//                                  status,
//                                  expected_record_count,
//                                  actual_loaded_record_count,
//                                  failure_reason
//                              FROM ingestion_execution
//                              WHERE execution_id = ?
//                              """
//                      )) {

//             statement.setObject(
//                     1,
//                     java.util.UUID.fromString(executionId)
//             );

//             try (ResultSet resultSet =
//                          statement.executeQuery()) {

//                 assertEquals(
//                         true,
//                         resultSet.next()
//                 );

//                 assertEquals(
//                         "employees.json",
//                         resultSet.getString("source_file")
//                 );

//                 assertEquals(
//                         "SUCCESS",
//                         resultSet.getString("status")
//                 );

//                 assertEquals(
//                         10L,
//                         resultSet.getLong(
//                                 "expected_record_count"
//                         )
//                 );

//                 assertEquals(
//                         10L,
//                         resultSet.getLong(
//                                 "actual_loaded_record_count"
//                         )
//                 );

//                 assertNull(
//                         resultSet.getString(
//                                 "failure_reason"
//                         )
//                 );
//             }
//         } finally {
//             deleteExecution(executionId);
//         }
//     }

//     private void deleteExecution(
//             String executionId)
//             throws Exception {

//         try (Connection connection =
//                      DriverManager.getConnection(
//                              JDBC_URL,
//                              USERNAME,
//                              PASSWORD
//                      );
//              PreparedStatement statement =
//                      connection.prepareStatement(
//                              """
//                              DELETE FROM ingestion_execution
//                              WHERE execution_id = ?
//                              """
//                      )) {

//             statement.setObject(
//                     1,
//                     java.util.UUID.fromString(executionId)
//             );

//             statement.executeUpdate();
//         }
//     }

//     private static class TestProcessContext
//             extends DoFnProcessContextAdapter {

//         private final IngestionExecution execution;

//         private TestProcessContext(
//                 IngestionExecution execution) {
//             this.execution = execution;
//         }

//         @Override
//         public IngestionExecution element() {
//             return execution;
//         }
//     }
// }
