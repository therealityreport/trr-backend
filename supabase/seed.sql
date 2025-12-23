begin;

-- Small but real seed data for end-to-end schema verification.
-- Note: user-owned tables (games.sessions/games.responses, surveys.responses/surveys.answers)
-- are intentionally NOT seeded because they require an authenticated user_id.

-- ---------------------------------------------------------------------------
-- core
-- ---------------------------------------------------------------------------

insert into core.shows (id, name, description, premiere_date)
values (
  'd1fdacc4-ccb0-4d52-8096-89889db83282',
  'TRR Sample Show',
  'Seed show for validating the core/games/surveys schemas.',
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

insert into core.episodes (id, season_id, episode_number, title, air_date)
values
  (
    '3d037712-54b6-4037-8109-1c69ab00448a',
    '2ea88321-cb37-4527-892f-0441030b6e68',
    1,
    'Episode 1: Kickoff',
    '2025-01-01'
  ),
  (
    '1a9ba2e1-031e-4279-a2fe-2f09deb8d2d0',
    '2ea88321-cb37-4527-892f-0441030b6e68',
    2,
    'Episode 2: Fallout',
    '2025-01-08'
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

insert into core.cast_memberships (id, show_id, season_id, person_id, role, billing_order)
values
  (
    '4338a8b0-b689-4dd0-9e87-87a7950eb540',
    'd1fdacc4-ccb0-4d52-8096-89889db83282',
    '2ea88321-cb37-4527-892f-0441030b6e68',
    '8ba911f0-777c-45c8-adad-5599624ad845',
    'cast',
    1
  ),
  (
    '1108bd61-5ef0-4f49-afd7-9964d090bf40',
    'd1fdacc4-ccb0-4d52-8096-89889db83282',
    '2ea88321-cb37-4527-892f-0441030b6e68',
    '69ce5e76-12c0-4a71-b426-1e8efaba3f0b',
    'cast',
    2
  ),
  (
    '668a80ac-f19c-47e8-9c83-98946b30ff72',
    'd1fdacc4-ccb0-4d52-8096-89889db83282',
    '2ea88321-cb37-4527-892f-0441030b6e68',
    '71d85ea4-d629-426d-b4fd-72777d8ae26c',
    'guest',
    3
  ),
  (
    '22c69b44-5362-4626-98cd-34bdb6ec68ae',
    'd1fdacc4-ccb0-4d52-8096-89889db83282',
    '2ea88321-cb37-4527-892f-0441030b6e68',
    '4f877630-0477-48f1-9ff4-ee0d296f6e7a',
    'host',
    0
  ),
  (
    'bedc02d5-8292-4993-a054-fd2f7ab40550',
    'd1fdacc4-ccb0-4d52-8096-89889db83282',
    '2ea88321-cb37-4527-892f-0441030b6e68',
    '37fe973a-3038-40b1-9622-b3f5f4f485ff',
    'cast',
    4
  )
on conflict (id) do nothing;

insert into core.episode_cast (id, episode_id, cast_membership_id)
values
  (
    '9a060f4a-fa33-4faf-896b-2ca598540d3a',
    '3d037712-54b6-4037-8109-1c69ab00448a',
    '4338a8b0-b689-4dd0-9e87-87a7950eb540'
  ),
  (
    '4ba12fbc-9ece-4db8-9bd8-9f067b3c6508',
    '3d037712-54b6-4037-8109-1c69ab00448a',
    '1108bd61-5ef0-4f49-afd7-9964d090bf40'
  ),
  (
    '02e5a066-980e-475f-87a2-9ef9e045a488',
    '3d037712-54b6-4037-8109-1c69ab00448a',
    '668a80ac-f19c-47e8-9c83-98946b30ff72'
  ),
  (
    'b1a6c04f-b449-47ff-aaa2-65f99b684e53',
    '3d037712-54b6-4037-8109-1c69ab00448a',
    '22c69b44-5362-4626-98cd-34bdb6ec68ae'
  ),
  (
    'e6a1e8da-ac9c-4a5a-ba1e-9d1bab58af7c',
    '3d037712-54b6-4037-8109-1c69ab00448a',
    'bedc02d5-8292-4993-a054-fd2f7ab40550'
  ),
  (
    '56d1b336-e945-45b2-820c-18c2840800b2',
    '1a9ba2e1-031e-4279-a2fe-2f09deb8d2d0',
    '4338a8b0-b689-4dd0-9e87-87a7950eb540'
  ),
  (
    'a4639d9f-a7f6-443c-928f-a6d72db4a064',
    '1a9ba2e1-031e-4279-a2fe-2f09deb8d2d0',
    '1108bd61-5ef0-4f49-afd7-9964d090bf40'
  ),
  (
    '39f6e886-1fa4-4446-afdb-8f31f2987329',
    '1a9ba2e1-031e-4279-a2fe-2f09deb8d2d0',
    '668a80ac-f19c-47e8-9c83-98946b30ff72'
  ),
  (
    '9891e619-de67-42ba-b129-71c678c851c3',
    '1a9ba2e1-031e-4279-a2fe-2f09deb8d2d0',
    '22c69b44-5362-4626-98cd-34bdb6ec68ae'
  ),
  (
    '0a8a702c-2c6b-40b2-86be-ba50ed31ed0d',
    '1a9ba2e1-031e-4279-a2fe-2f09deb8d2d0',
    'bedc02d5-8292-4993-a054-fd2f7ab40550'
  )
on conflict (id) do nothing;

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
-- games
-- ---------------------------------------------------------------------------

insert into games.games (id, show_id, season_id, episode_id, game_type, title, description, status, starts_at)
values (
  '44c17a72-6264-496c-ad0f-24e374e2ba39',
  'd1fdacc4-ccb0-4d52-8096-89889db83282',
  '2ea88321-cb37-4527-892f-0441030b6e68',
  '3d037712-54b6-4037-8109-1c69ab00448a',
  'quiz',
  'Episode 1 Quick Quiz',
  'Seed game for validation (questions/options + answer keys).',
  'published',
  now()
)
on conflict (id) do nothing;

insert into games.questions (id, game_id, question_order, prompt, question_type)
values
  (
    '5925bde7-82c9-4b63-acfa-2c7757b2182c',
    '44c17a72-6264-496c-ad0f-24e374e2ba39',
    1,
    'Who hosted Episode 1?',
    'single_choice'
  ),
  (
    'f371e10f-179d-4e32-9286-cbbe0d21b3fd',
    '44c17a72-6264-496c-ad0f-24e374e2ba39',
    2,
    'Pick the best description of the opening scene.',
    'single_choice'
  )
on conflict (id) do nothing;

insert into games.options (id, question_id, option_order, label, value)
values
  ('65eeaefb-9c3c-4c56-9f9d-a1481f3fc8f4', '5925bde7-82c9-4b63-acfa-2c7757b2182c', 1, 'Drew Patel', 'drew'),
  ('d95c9eb2-556b-4f9b-8e83-fa8cdb3c4b0a', '5925bde7-82c9-4b63-acfa-2c7757b2182c', 2, 'Ava Stone', 'ava'),
  ('8c2a9367-6fb9-4048-8e03-98fc3e38c3cc', '5925bde7-82c9-4b63-acfa-2c7757b2182c', 3, 'Ben Carter', 'ben'),
  ('0074fd18-1b9a-45ef-b77e-79e8e0d770e3', '5925bde7-82c9-4b63-acfa-2c7757b2182c', 4, 'Casey Nguyen', 'casey'),
  ('f3ba48d7-0345-46ae-b4bc-865b69d4af62', 'f371e10f-179d-4e32-9286-cbbe0d21b3fd', 1, 'A surprise arrival', 'surprise'),
  ('b4a1b9d0-86fb-4f0b-a61e-e31c1b6e3d3e', 'f371e10f-179d-4e32-9286-cbbe0d21b3fd', 2, 'A tense argument', 'argument'),
  ('459b73ab-ad23-49db-a0e6-326fe563067a', 'f371e10f-179d-4e32-9286-cbbe0d21b3fd', 3, 'A group challenge', 'challenge'),
  ('75ee4598-568f-4824-bb71-1716ebecf8b2', 'f371e10f-179d-4e32-9286-cbbe0d21b3fd', 4, 'A flashback montage', 'flashback')
on conflict (id) do nothing;

insert into games.answer_keys (id, question_id, answer, explanation)
values
  (
    '000504db-dbda-4086-a982-4d6c4a1a56a6',
    '5925bde7-82c9-4b63-acfa-2c7757b2182c',
    '{"correct_option_id":"65eeaefb-9c3c-4c56-9f9d-a1481f3fc8f4"}'::jsonb,
    'Host credit from seed cast list.'
  ),
  (
    'c35a3602-96c5-4c7e-81aa-0bcffdf0bcb4',
    'f371e10f-179d-4e32-9286-cbbe0d21b3fd',
    '{"correct_option_id":"f3ba48d7-0345-46ae-b4bc-865b69d4af62"}'::jsonb,
    'Arbitrary seed answer for validation.'
  )
on conflict (id) do nothing;

insert into games.stats (id, game_id, question_id, stats)
values
  (
    '33e28107-2056-402c-afca-c41e0a07dfe2',
    '44c17a72-6264-496c-ad0f-24e374e2ba39',
    null,
    '{"total":0}'::jsonb
  ),
  (
    '0e7c4468-ea31-4543-8a83-816ac391140d',
    '44c17a72-6264-496c-ad0f-24e374e2ba39',
    '5925bde7-82c9-4b63-acfa-2c7757b2182c',
    '{"total":0,"by_option":{}}'::jsonb
  ),
  (
    'b1348709-18ea-415f-b7e2-1e4858cc19b8',
    '44c17a72-6264-496c-ad0f-24e374e2ba39',
    'f371e10f-179d-4e32-9286-cbbe0d21b3fd',
    '{"total":0,"by_option":{}}'::jsonb
  )
on conflict (id) do nothing;

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
