begin;

do $$
begin
  if to_regclass('core.show_source_history_id_seq') is not null then
    grant usage, select on sequence core.show_source_history_id_seq to service_role;
  end if;
  if to_regclass('core.season_source_history_id_seq') is not null then
    grant usage, select on sequence core.season_source_history_id_seq to service_role;
  end if;
  if to_regclass('core.episode_source_history_id_seq') is not null then
    grant usage, select on sequence core.episode_source_history_id_seq to service_role;
  end if;
  if to_regclass('core.person_source_history_id_seq') is not null then
    grant usage, select on sequence core.person_source_history_id_seq to service_role;
  end if;
end $$;

commit;
