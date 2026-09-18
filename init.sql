CREATE TABLE IF NOT EXISTS notifications
(
    id             UUID         DEFAULT gen_random_uuid() PRIMARY KEY,
    correlation_id VARCHAR(255) UNIQUE NOT NULL,
    channel        VARCHAR(50)  NOT NULL,
    recipient      VARCHAR(500) NOT NULL,
    subject        VARCHAR(1000),
    content        TEXT         NOT NULL,
    status         VARCHAR(50)  NOT NULL DEFAULT 'PENDING',
    priority       VARCHAR(50)  NOT NULL DEFAULT 'NORMAL',
    retry_count    INTEGER               DEFAULT 0,
    error_message  TEXT,
    created_at     TIMESTAMP             DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP             DEFAULT CURRENT_TIMESTAMP,
    version        BIGINT         DEFAULT 0 NOT NULL
);

CREATE TABLE IF NOT EXISTS delivery_attempts
(
    id              UUID    DEFAULT gen_random_uuid() PRIMARY KEY,
    notification_id UUID    NOT NULL REFERENCES notifications (id),
    attempt_number  INTEGER NOT NULL,
    status          VARCHAR(50) NOT NULL,
    error_message   TEXT,
    attempted_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_notif_correlation ON notifications (correlation_id);
CREATE INDEX IF NOT EXISTS idx_notif_status ON notifications (status);
CREATE INDEX IF NOT EXISTS idx_notif_channel ON notifications (channel);
CREATE INDEX IF NOT EXISTS idx_attempts_notif_id ON delivery_attempts (notification_id);