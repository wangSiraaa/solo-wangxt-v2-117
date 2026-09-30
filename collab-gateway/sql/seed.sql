-- 演示种子数据（幂等）。
INSERT INTO tenants (id, name) VALUES
  ('t-acme', 'Acme'),
  ('t-globex', 'Globex')
ON CONFLICT (id) DO NOTHING;

INSERT INTO app_users (id, tenant_id, display_name, token) VALUES
  ('u-alice',   't-acme',   'Alice',   'tok-alice'),
  ('u-bob',     't-acme',   'Bob',     'tok-bob'),
  ('u-carol',   't-acme',   'Carol',   'tok-carol'),
  ('u-dave',    't-globex', 'Dave',    'tok-dave')
ON CONFLICT (id) DO NOTHING;

INSERT INTO documents (id, tenant_id, title) VALUES
  ('doc-note',  't-acme',   'Sprint Notes'),
  ('doc-plan',  't-acme',   'Roadmap'),
  ('doc-secret','t-globex', 'Globex Internal')
ON CONFLICT (id) DO NOTHING;

-- Alice/Bob 可编辑 note；Carol 只读；Globex 的 Dave 无权访问
INSERT INTO document_members (doc_id, user_id, role) VALUES
  ('doc-note',  'u-alice', 'owner'),
  ('doc-note',  'u-bob',   'editor'),
  ('doc-note',  'u-carol', 'viewer'),
  ('doc-plan',  'u-alice', 'owner'),
  ('doc-secret','u-dave',  'owner')
ON CONFLICT (doc_id, user_id) DO NOTHING;
