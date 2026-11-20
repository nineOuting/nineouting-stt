"""이 파드가 새 회의를 받을지 정함 (SCRUM-337). server.py의 정리 스레드가 5초마다 부르고, /ready가 결과를 읽음.

파드가 여러 대이고 ALB 쿠키 고정(스티키 세션)을 쓰면, 꽉 찬 파드에 고정된 브라우저는 새 회의를 열 때마다 1013으로 거절당함.
꽉 차거나 CPU가 못 따라가면 /ready를 503으로 내서 ALB 대상에서 빠지게 하면, 새 연결은 다른 파드로 가고 이미 붙은 연결은 그대로 남음.
파드가 1대일 때는 갈 곳이 없어 진행 중인 회의의 다시 연결까지 막히므로, server.py는 READY_WHEN_FULL을 켰을 때만 이 결과를 씀.
"""


def decide(was_busy, n, cap, margin, rooms, lag_sec):
    """→ (busy, 이유)

    was_busy: 지난번 결과
    n, cap: 열린 회의 수, 상한 (0이면 상한 없음)
    margin: 꽉 찬 뒤 몇 자리가 비어야 다시 받을지 (1 미만은 1로 봄)
    rooms: [(회의 번호, 밀린 소리 초, 지난번 뒤로 처리한 소리 초, 지난번 뒤로 지난 시간 초)]
           처리한 소리를 모르면(엔진이 아직 없음 등) 지난 시간과 같은 값을 넣어 밀림으로 보지 않게 함
    lag_sec: 밀린 소리가 이 초를 넘고 실시간보다 느리게 처리하면 밀림

    밀림은 밀린 소리 크기만 보지 않음: 다시 붙은 회의가 모아 둔 소리를 몰아 보내면 CPU가 남아도 밀린 소리가 잠깐 커짐.
    이때는 실시간보다 빨리 처리하므로 밀림으로 보지 않고, CPU가 모자라 실시간보다 느릴 때만 밀림으로 봄.
    """
    margin = max(1, margin)
    full = cap > 0 and n >= cap
    lagging = [(b, k) for k, b, done, spent in rooms if b > lag_sec and spent > 0 and done < spent]
    if not was_busy:
        if full: return True, f'회의 {n}개 (상한 {cap})'
        if lagging:
            b, k = max(lagging)
            return True, f'회의 {k} 밀림 {b:.1f}초 (실시간보다 느리게 처리함)'
        return False, ''
    free = cap <= 0 or n <= cap - margin
    worst = max(((b, k) for k, b, _, _ in rooms), default=(0.0, None))
    if free and worst[0] < 1.0: return False, ''
    if not free: return True, f'회의 {n}개 (상한 {cap}, {cap - margin}개 이하가 되면 다시 받음)'
    return True, f'회의 {worst[1]} 밀린 소리 {worst[0]:.1f}초 (1초 밑으로 내려오면 다시 받음)'
