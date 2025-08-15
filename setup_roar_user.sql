-- SQL commands to run on your RDS instance
-- Connect to your RDS instance using a tool like pgAdmin or psql from local machine

-- 1. Connect to postgres database first
\c postgres;

-- 2. Create a ROAR-specific user that can access thebigone database
CREATE USER roar_user WITH PASSWORD 'roar_secure_password_123';

-- 3. Switch to thebigone database
\c thebigone;

-- 4. Grant necessary permissions to the ROAR user
GRANT CONNECT ON DATABASE thebigone TO roar_user;
GRANT USAGE ON SCHEMA public TO roar_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO roar_user;
GRANT SELECT, USAGE ON ALL SEQUENCES IN SCHEMA public TO roar_user;

-- 5. Grant permissions on future tables (for new tables created by your scripts)
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO roar_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, USAGE ON SEQUENCES TO roar_user;
