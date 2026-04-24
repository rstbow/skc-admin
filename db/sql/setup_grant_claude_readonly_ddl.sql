/* =============================================================================
   One-time setup: grant DDL-only permissions to claude_readonly.

   Run BOTH sections in SSMS as an admin login. After this lands, Claude
   can run schema migrations (CREATE TABLE, ALTER TABLE, CREATE VIEW,
   CREATE/DROP PROCEDURE, CREATE INDEX) without human intervention.

   What claude_readonly WILL be able to do after this:
     - Create new tables, views, procs, indexes in admin / raw / curated
     - Add columns to existing admin / raw / curated tables
     - Drop + recreate procs (incl. preserving grants via CREATE OR ALTER)
     - Add / alter constraints within these schemas
     - Read any table (retains existing SELECT)

   What it still CANNOT do (by design):
     - INSERT / UPDATE / DELETE on any existing table
     - EXECUTE stored procedures (can't invoke procs that mutate)
     - Modify logins, users, or roles
     - Touch other schemas (dbo, sys, INFORMATION_SCHEMA)
     - Grant permissions to anyone else

   Seed data migrations (MERGEs into admin.Connectors, admin.Jobs,
   admin.ErrorRunbooks, etc.) still need the human in the loop.

   To roll back, use the REVOKE block at the bottom.
   ============================================================================= */


/* ============================================================
   Part 1 — skc-admin database
   ============================================================ */

USE [skc-admin];
GO

GRANT CREATE TABLE                     TO claude_readonly;
GRANT CREATE VIEW                      TO claude_readonly;
GRANT ALTER      ON SCHEMA::admin      TO claude_readonly;
GRANT REFERENCES ON SCHEMA::admin      TO claude_readonly;
GO

PRINT 'skc-admin: claude_readonly has DDL on admin schema.';
GO


/* ============================================================
   Part 2 — vs-ims-staging database

   Also creates the curated schema (owned by dbo) if missing, so the
   follow-up CREATE VIEW in migration 023 lands cleanly on re-run.
   ============================================================ */

USE [vs-ims-staging];
GO

IF SCHEMA_ID('curated') IS NULL
BEGIN
    EXEC('CREATE SCHEMA curated AUTHORIZATION dbo');
    PRINT 'vs-ims-staging: created schema curated (owner = dbo).';
END
GO

GRANT CREATE TABLE                     TO claude_readonly;
GRANT CREATE VIEW                      TO claude_readonly;
GRANT CREATE PROCEDURE                 TO claude_readonly;
GRANT ALTER      ON SCHEMA::raw        TO claude_readonly;
GRANT ALTER      ON SCHEMA::curated    TO claude_readonly;
GRANT REFERENCES ON SCHEMA::raw        TO claude_readonly;
GRANT REFERENCES ON SCHEMA::curated    TO claude_readonly;
GO

PRINT 'vs-ims-staging: claude_readonly has DDL on raw + curated schemas.';
GO


/* ============================================================
   Rollback (uncomment if you want to revoke, then run in SSMS)
   ============================================================ */

/*
USE [skc-admin];
REVOKE CREATE TABLE FROM claude_readonly;
REVOKE CREATE VIEW  FROM claude_readonly;
REVOKE ALTER      ON SCHEMA::admin   FROM claude_readonly;
REVOKE REFERENCES ON SCHEMA::admin   FROM claude_readonly;
GO

USE [vs-ims-staging];
REVOKE CREATE TABLE     FROM claude_readonly;
REVOKE CREATE VIEW      FROM claude_readonly;
REVOKE CREATE PROCEDURE FROM claude_readonly;
REVOKE ALTER      ON SCHEMA::raw      FROM claude_readonly;
REVOKE ALTER      ON SCHEMA::curated  FROM claude_readonly;
REVOKE REFERENCES ON SCHEMA::raw      FROM claude_readonly;
REVOKE REFERENCES ON SCHEMA::curated  FROM claude_readonly;
GO
*/
