begin;

-- Small but real seed data for end-to-end schema verification.
-- Note: user-owned tables (surveys.responses/surveys.answers) are intentionally NOT seeded
-- because they require an authenticated user_id.

-- ---------------------------------------------------------------------------
-- core
-- ---------------------------------------------------------------------------

insert into core.shows (id, name, description, premiere_date)
values (
  'd1fdacc4-ccb0-4d52-8096-89889db83282',
  'TRR Sample Show',
  'Seed show for validating the core/surveys schemas.',
  '2025-01-01'
)
on conflict (id) do nothing;

insert into core.seasons (id, show_id, season_number, title, premiere_date)
values (
  '2ea88321-cb37-4527-892f-0441030b6e68',
  'd1fdacc4-ccb0-4d52-8096-89889db83282',
  1,
  'Season 1',
  '2025-01-01'
)
on conflict (id) do nothing;

insert into core.episodes (
  id,
  show_id,
  season_id,
  season_number,
  episode_number,
  title,
  air_date,
  show_name
)
values
  (
    '3d037712-54b6-4037-8109-1c69ab00448a',
    'd1fdacc4-ccb0-4d52-8096-89889db83282',
    '2ea88321-cb37-4527-892f-0441030b6e68',
    1,
    1,
    'Episode 1: Kickoff',
    '2025-01-01',
    'TRR Sample Show'
  ),
  (
    '1a9ba2e1-031e-4279-a2fe-2f09deb8d2d0',
    'd1fdacc4-ccb0-4d52-8096-89889db83282',
    '2ea88321-cb37-4527-892f-0441030b6e68',
    1,
    2,
    'Episode 2: Fallout',
    '2025-01-08',
    'TRR Sample Show'
  )
on conflict (id) do nothing;

insert into core.people (id, full_name, known_for)
values
  ('8ba911f0-777c-45c8-adad-5599624ad845', 'Ava Stone', 'Cast'),
  ('69ce5e76-12c0-4a71-b426-1e8efaba3f0b', 'Ben Carter', 'Cast'),
  ('71d85ea4-d629-426d-b4fd-72777d8ae26c', 'Casey Nguyen', 'Guest'),
  ('4f877630-0477-48f1-9ff4-ee0d296f6e7a', 'Drew Patel', 'Host'),
  ('37fe973a-3038-40b1-9622-b3f5f4f485ff', 'Emery Brooks', 'Cast')
on conflict (id) do nothing;

-- Seed credits model (canonical show-level cast membership)
insert into core.credits (id, show_id, person_id, credit_category, role, billing_order, source_type, metadata)
values
  (
    '4338a8b0-b689-4dd0-9e87-87a7950eb540',
    'd1fdacc4-ccb0-4d52-8096-89889db83282',
    '8ba911f0-777c-45c8-adad-5599624ad845',
    'Self',
    'cast',
    1,
    'manual',
    '{}'::jsonb
  ),
  (
    '1108bd61-5ef0-4f49-afd7-9964d090bf40',
    'd1fdacc4-ccb0-4d52-8096-89889db83282',
    '69ce5e76-12c0-4a71-b426-1e8efaba3f0b',
    'Self',
    'cast',
    2,
    'manual',
    '{}'::jsonb
  ),
  (
    '668a80ac-f19c-47e8-9c83-98946b30ff72',
    'd1fdacc4-ccb0-4d52-8096-89889db83282',
    '71d85ea4-d629-426d-b4fd-72777d8ae26c',
    'Self',
    'guest',
    3,
    'manual',
    '{}'::jsonb
  ),
  (
    '22c69b44-5362-4626-98cd-34bdb6ec68ae',
    'd1fdacc4-ccb0-4d52-8096-89889db83282',
    '4f877630-0477-48f1-9ff4-ee0d296f6e7a',
    'Self',
    'host',
    0,
    'manual',
    '{}'::jsonb
  ),
  (
    'bedc02d5-8292-4993-a054-fd2f7ab40550',
    'd1fdacc4-ccb0-4d52-8096-89889db83282',
    '37fe973a-3038-40b1-9622-b3f5f4f485ff',
    'Self',
    'cast',
    4,
    'manual',
    '{}'::jsonb
  )
on conflict (id) do nothing;

-- Seed episode-level presence via credit_occurrences
insert into core.credit_occurrences (credit_id, episode_id, appearance_type)
values
  (
    '4338a8b0-b689-4dd0-9e87-87a7950eb540',
    '3d037712-54b6-4037-8109-1c69ab00448a',
    'appears'
  ),
  (
    '1108bd61-5ef0-4f49-afd7-9964d090bf40',
    '3d037712-54b6-4037-8109-1c69ab00448a',
    'appears'
  ),
  (
    '668a80ac-f19c-47e8-9c83-98946b30ff72',
    '3d037712-54b6-4037-8109-1c69ab00448a',
    'appears'
  ),
  (
    '22c69b44-5362-4626-98cd-34bdb6ec68ae',
    '3d037712-54b6-4037-8109-1c69ab00448a',
    'appears'
  ),
  (
    'bedc02d5-8292-4993-a054-fd2f7ab40550',
    '3d037712-54b6-4037-8109-1c69ab00448a',
    'appears'
  ),
  (
    '4338a8b0-b689-4dd0-9e87-87a7950eb540',
    '1a9ba2e1-031e-4279-a2fe-2f09deb8d2d0',
    'appears'
  ),
  (
    '1108bd61-5ef0-4f49-afd7-9964d090bf40',
    '1a9ba2e1-031e-4279-a2fe-2f09deb8d2d0',
    'appears'
  ),
  (
    '668a80ac-f19c-47e8-9c83-98946b30ff72',
    '1a9ba2e1-031e-4279-a2fe-2f09deb8d2d0',
    'appears'
  ),
  (
    '22c69b44-5362-4626-98cd-34bdb6ec68ae',
    '1a9ba2e1-031e-4279-a2fe-2f09deb8d2d0',
    'appears'
  ),
  (
    'bedc02d5-8292-4993-a054-fd2f7ab40550',
    '1a9ba2e1-031e-4279-a2fe-2f09deb8d2d0',
    'appears'
  )
on conflict (credit_id, episode_id) do nothing;

-- ---------------------------------------------------------------------------
-- surveys
-- ---------------------------------------------------------------------------

insert into surveys.surveys (id, show_id, season_id, episode_id, title, description, status, starts_at)
values (
  '8a24c95d-93bc-4297-9c84-7946b753eb2d',
  'd1fdacc4-ccb0-4d52-8096-89889db83282',
  '2ea88321-cb37-4527-892f-0441030b6e68',
  '3d037712-54b6-4037-8109-1c69ab00448a',
  'Episode 1 Viewer Poll',
  'Seed survey for validation (questions/options + aggregates).',
  'published',
  now()
)
on conflict (id) do nothing;

insert into surveys.questions (id, survey_id, question_order, prompt, question_type)
values
  (
    'c5106823-7875-43d9-9172-4fbaa076a2b9',
    '8a24c95d-93bc-4297-9c84-7946b753eb2d',
    1,
    'Who was MVP of Episode 1?',
    'single_choice'
  ),
  (
    'faf122a0-d63f-4250-8431-5b7dadb80f53',
    '8a24c95d-93bc-4297-9c84-7946b753eb2d',
    2,
    'Rate the episode overall.',
    'single_choice'
  ),
  (
    'bfc0b968-7d5a-4826-a57d-82c9ac872226',
    '8a24c95d-93bc-4297-9c84-7946b753eb2d',
    3,
    'Any quick thoughts?',
    'free_text'
  )
on conflict (id) do nothing;

insert into surveys.options (id, question_id, option_order, label, value)
values
  ('e45aa52e-5d8f-467e-a3fd-7bad3df8f2e3', 'c5106823-7875-43d9-9172-4fbaa076a2b9', 1, 'Ava Stone', 'ava'),
  ('aa2549db-8c66-4815-8a24-4ff72e660eea', 'c5106823-7875-43d9-9172-4fbaa076a2b9', 2, 'Ben Carter', 'ben'),
  ('5163cac9-7c44-4a06-8cda-338d767f1e2c', 'c5106823-7875-43d9-9172-4fbaa076a2b9', 3, 'Drew Patel', 'drew'),
  ('9107574e-bbbc-46f5-8daf-43cd2dc5f3d8', 'faf122a0-d63f-4250-8431-5b7dadb80f53', 1, '1', '1'),
  ('5589b290-f954-46da-a38c-08f84e890b8d', 'faf122a0-d63f-4250-8431-5b7dadb80f53', 2, '2', '2'),
  ('6d906331-14ab-452c-aa2a-6216e1957900', 'faf122a0-d63f-4250-8431-5b7dadb80f53', 3, '3', '3'),
  ('1f3bdd84-b9e9-4ce6-ac97-b014a14eca6d', 'faf122a0-d63f-4250-8431-5b7dadb80f53', 4, '4', '4'),
  ('841022cc-3c1b-4e1e-9509-dd74f9fe2700', 'faf122a0-d63f-4250-8431-5b7dadb80f53', 5, '5', '5')
on conflict (id) do nothing;

insert into surveys.aggregates (survey_id, question_id, aggregate, updated_at)
values
  (
    '8a24c95d-93bc-4297-9c84-7946b753eb2d',
    'c5106823-7875-43d9-9172-4fbaa076a2b9',
    '{"total":0,"by_option":{}}'::jsonb,
    now()
  ),
  (
    '8a24c95d-93bc-4297-9c84-7946b753eb2d',
    'faf122a0-d63f-4250-8431-5b7dadb80f53',
    '{"total":0,"by_option":{}}'::jsonb,
    now()
  ),
  (
    '8a24c95d-93bc-4297-9c84-7946b753eb2d',
    'bfc0b968-7d5a-4826-a57d-82c9ac872226',
    '{"total":0}'::jsonb,
    now()
  )
on conflict (survey_id, question_id) do nothing;

-- ---------------------------------------------------------------------------
-- social (discussions)
-- Note: created_by/user_id are NULL for seed data since we don't have auth users
-- ---------------------------------------------------------------------------

insert into social.threads (id, episode_id, title, type, created_by, is_locked)
values (
  'a1b2c3d4-e5f6-7890-abcd-ef1234567890',
  '3d037712-54b6-4037-8109-1c69ab00448a',
  'Episode 1 Live Discussion Thread',
  'episode_live',
  null,
  false
)
on conflict (id) do nothing;

insert into social.posts (id, thread_id, parent_post_id, user_id, body)
values
  (
    'b2c3d4e5-f6a7-8901-bcde-f23456789012',
    'a1b2c3d4-e5f6-7890-abcd-ef1234567890',
    null,
    null,
    'Can''t believe that opening scene! Ava absolutely killed it.'
  ),
  (
    'c3d4e5f6-a7b8-9012-cdef-345678901234',
    'a1b2c3d4-e5f6-7890-abcd-ef1234567890',
    null,
    null,
    'Drew''s hosting is top tier this season. The energy is unmatched!'
  ),
  (
    'd4e5f6a7-b8c9-0123-defa-456789012345',
    'a1b2c3d4-e5f6-7890-abcd-ef1234567890',
    'b2c3d4e5-f6a7-8901-bcde-f23456789012',
    null,
    'Right?! She really came to play. My MVP vote is locked in.'
  )
on conflict (id) do nothing;

-- Note: reactions require user_id which cannot be null per the schema
-- Skipping reaction seed data since we need real authenticated users

commit;
