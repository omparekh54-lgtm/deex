"""Bounded sequential catch-up; only actual successful collectors can complete."""
def drain_collection(collect_once, locked, *, max_passes=6, on_progress=None):
    if not 1<=max_passes<=8:raise ValueError('max_passes must be 1..8')
    passes=[];previous=None
    for number in range(max_passes):
        locked()
        result=collect_once()
        stages=result.get('stages') or {}
        queues=[stages.get(k) or {} for k in ('financials','shareholding')]
        remaining=sum(q.get('remaining') or 0 for q in queues)
        errors=[e for q in queues for e in q.get('errors',[])]
        progress=dict(pass_number=number+1,run_id=result.get('run_id'),status=result.get('status'),
            complete=result.get('complete') is True,remaining=remaining,errors=errors)
        passes.append(progress)
        if on_progress:on_progress(progress)
        if result.get('status')=='SUCCESS' and result.get('complete') is True:break
        # Do not retry hard failures, lock contention, non-queue incompleteness,
        # source errors or a stalled backlog in this bounded repair invocation.
        if (result.get('status')!='PARTIAL' or errors or remaining==0
                or sum(q.get('processed') or 0 for q in queues)==0
                or (previous is not None and remaining>=previous)):
            break
        previous=remaining
    return {**result,'collector_passes':passes}
