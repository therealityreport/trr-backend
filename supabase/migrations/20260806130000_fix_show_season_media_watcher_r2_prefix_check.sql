begin;

alter table core.show_season_media_watches
  drop constraint show_season_media_watches_r2_prefix_check;

alter table core.show_season_media_watches
  add constraint show_season_media_watches_r2_prefix_check
  check (
    char_length(r2_prefix) between 1 and 512
    and r2_prefix ~ '^[A-Za-z0-9][A-Za-z0-9._/-]*$'
    and position('..' in r2_prefix) = 0
  );

commit;
