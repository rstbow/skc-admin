/* =============================================================================
   Seed the initial admin user.
   Do NOT run this directly — it's templated.
   The Node scaffold has `scripts/seed.js` that:
     1. Prompts for password (or reads from env)
     2. bcrypt-hashes it
     3. Inserts via parameterized query

   If you need to seed manually (one-time bootstrap), generate a bcrypt hash
   from a Node REPL:
       const bcrypt = require('bcrypt');
       bcrypt.hashSync('your-password-here', 12);

   Then paste the resulting hash below and run.
   ============================================================================= */

DECLARE @Email          NVARCHAR(320)  = N'randy@skucompass.com';
DECLARE @DisplayName    NVARCHAR(100)  = N'Randy';
DECLARE @PasswordHash   NVARCHAR(200)  = N'<<PASTE_BCRYPT_HASH_HERE>>';

IF NOT EXISTS (SELECT 1 FROM admin.Users WHERE Email = @Email)
BEGIN
    INSERT INTO admin.Users (Email, DisplayName, PasswordHash, IsSuperAdmin)
    VALUES (@Email, @DisplayName, @PasswordHash, 1);

    PRINT 'Admin user seeded: ' + @Email;
END
ELSE
BEGIN
    PRINT 'Admin user already exists, skipping.';
END
GO
