package com.amex.lumi.ingestion.exception;

import com.amex.lumi.ingestion.dto.ApiErrorResponse;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.FieldError;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.resource.NoResourceFoundException;

import java.time.Instant;
import java.util.stream.Collectors;

/**
 * Turns exceptions into JSON error responses with the right HTTP status and error code.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    /** All of this API's own exceptions: the error code gives the status. */
    @ExceptionHandler(LumiException.class)
    public ResponseEntity<ApiErrorResponse> handleLumi(LumiException exception, HttpServletRequest request) {
        // Airflow errors are already logged by the client; others are logged here.
        if (exception.code().status().is5xxServerError() && exception.code() != ErrorCode.AIRFLOW_UNAVAILABLE) {
            LOGGER.error("{} on {} {}", exception.code(), request.getMethod(), request.getRequestURI(), exception);
        }
        return build(exception.code(), exception.getMessage(), request);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiErrorResponse> handleValidation(MethodArgumentNotValidException exception,
                                                             HttpServletRequest request) {
        String message = exception.getBindingResult().getFieldErrors().stream()
                .map(FieldError::getDefaultMessage)
                .collect(Collectors.joining(", "));
        return build(ErrorCode.VALIDATION_FAILED, message, request);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ApiErrorResponse> handleUnreadableBody(HttpMessageNotReadableException exception,
                                                                 HttpServletRequest request) {
        return build(ErrorCode.MALFORMED_BODY, "Request body is not valid JSON or fileType is not CSV/JSON",
                request);
    }

    // e.g. GET /api/v1/ingestions/abc where a UUID is expected.
    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<ApiErrorResponse> handleWrongPathValue(MethodArgumentTypeMismatchException exception,
                                                                 HttpServletRequest request) {
        return build(ErrorCode.INVALID_PATH_VALUE,
                exception.getName() + " has an invalid value: " + exception.getValue(), request);
    }

    // Spring's own errors, which would otherwise become a wrong 500.
    @ExceptionHandler(NoResourceFoundException.class)
    public ResponseEntity<ApiErrorResponse> handleUnknownUrl(NoResourceFoundException exception,
                                                             HttpServletRequest request) {
        return build(ErrorCode.ENDPOINT_NOT_FOUND,
                "No endpoint " + request.getMethod() + " " + request.getRequestURI(), request);
    }

    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    public ResponseEntity<ApiErrorResponse> handleWrongMethod(HttpRequestMethodNotSupportedException exception,
                                                              HttpServletRequest request) {
        String[] supported = exception.getSupportedMethods() == null ? new String[0] : exception.getSupportedMethods();
        return build(ErrorCode.METHOD_NOT_ALLOWED,
                exception.getMethod() + " is not supported here; use " + String.join(" or ", supported), request);
    }

    @ExceptionHandler(HttpMediaTypeNotSupportedException.class)
    public ResponseEntity<ApiErrorResponse> handleWrongContentType(HttpMediaTypeNotSupportedException exception,
                                                                   HttpServletRequest request) {
        return build(ErrorCode.UNSUPPORTED_MEDIA_TYPE,
                "Content type " + exception.getContentType() + " is not supported; send application/json", request);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiErrorResponse> handleUnexpected(Exception exception, HttpServletRequest request) {
        LOGGER.error("Unexpected error on {} {}", request.getMethod(), request.getRequestURI(), exception);
        // Hide internal details from the client; they are in the log.
        return build(ErrorCode.INTERNAL_ERROR, "An unexpected error occurred", request);
    }

    // Client mistakes (4xx) are WARN; 5xx errors are logged as ERROR above.
    private ResponseEntity<ApiErrorResponse> build(ErrorCode code, String message, HttpServletRequest request) {
        HttpStatus status = code.status();
        if (status.is4xxClientError()) {
            LOGGER.warn("{} {} rejected with {} {}: {}", request.getMethod(), request.getRequestURI(),
                    status.value(), code, message);
        }
        ApiErrorResponse body = new ApiErrorResponse(Instant.now(), status.value(), status.getReasonPhrase(),
                code.name(), message, request.getRequestURI());
        return ResponseEntity.status(status).body(body);
    }
}
