-- ========================================
-- Database Schema - Webhook Scheduler
-- ========================================

CREATE TABLE IF NOT EXISTS scheduled_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
    
    url TEXT NOT NULL CHECK (url ~* '^https?://'),
    http_method VARCHAR(10) NOT NULL DEFAULT 'POST' 
        CHECK (http_method IN ('GET', 'POST', 'PUT', 'PATCH', 'DELETE')),
    request_headers JSONB DEFAULT '{}',
    request_body JSONB,
    
    schedule_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    status VARCHAR(20) NOT NULL DEFAULT 'pending' 
        CHECK (status IN ('pending', 'processing', 'completed', 'failed', 'cancelled')),
    
    attempt_count INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 5,
    
    worker_id UUID,
    started_at TIMESTAMPTZ,
    
    last_attempt_at TIMESTAMPTZ,
    last_response_status_code INT,
    last_error_message TEXT,
    last_response_body TEXT,
    
    idempotency_key VARCHAR(255) UNIQUE NOT NULL,
    
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS job_executions (
    job_id UUID REFERENCES scheduled_jobs(id) ON DELETE CASCADE,
    attempt_num INT NOT NULL,
    
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at TIMESTAMPTZ,
    duration_ms INTEGER,
    
    response_status_code INT,
    error_message TEXT,
    error_stack_trace TEXT,
    body_response TEXT,
    
    worker_id UUID,
    
    CONSTRAINT pk_job_execution PRIMARY KEY (job_id, attempt_num)
);

-- Índices
CREATE INDEX IF NOT EXISTS idx_pending_jobs ON scheduled_jobs(status, schedule_at) 
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_user_jobs ON scheduled_jobs(tenant_id, schedule_at DESC);

CREATE INDEX IF NOT EXISTS idx_processing_jobs ON scheduled_jobs(id) 
    WHERE status = 'processing';

CREATE INDEX IF NOT EXISTS idx_idempotency ON scheduled_jobs(idempotency_key);

CREATE INDEX IF NOT EXISTS idx_execution_job ON job_executions(job_id DESC);
CREATE INDEX IF NOT EXISTS idx_execution_started ON job_executions(started_at DESC);