select
  post.label,
  post.queryid,
  base.calls as baseline_calls,
  post.calls as post_calls,
  case when base.calls > 0 then base.total_exec_time / base.calls else null end as baseline_mean_exec_time,
  case when post.calls > 0 then post.total_exec_time / post.calls else null end as post_mean_exec_time,
  case
    when base.calls >= 20 and post.calls >= 20 and base.total_exec_time > 0
      then (post.total_exec_time / post.calls) / (base.total_exec_time / base.calls)
    else null
  end as regression_ratio,
  case
    when base.calls < 20 or post.calls < 20 then 'insufficient_calls'
    when (post.total_exec_time / post.calls) > ((base.total_exec_time / base.calls) * 1.25) then 'regressed'
    else 'ok'
  end as status
from baseline_snapshot base
join post_snapshot post on post.label = base.label and post.queryid = base.queryid
order by post.label, post.queryid;
