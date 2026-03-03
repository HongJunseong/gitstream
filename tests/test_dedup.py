from poller.utils.dedup import EventState


def test_filter_new_events_no_last_id(tmp_path):
    """last_event_id 없으면 전체 이벤트 반환."""
    state = EventState(state_file=str(tmp_path / "state.json"))
    events = [{"id": "3"}, {"id": "2"}, {"id": "1"}]
    assert state.filter_new_events(events) == events


def test_filter_new_events_with_last_id(tmp_path):
    """last_event_id 이후 신규 이벤트만 반환."""
    state = EventState(state_file=str(tmp_path / "state.json"))
    state.last_event_id = "2"
    events = [{"id": "4"}, {"id": "3"}, {"id": "2"}, {"id": "1"}]
    assert state.filter_new_events(events) == [{"id": "4"}, {"id": "3"}]


def test_filter_new_events_unknown_last_id(tmp_path):
    """last_event_id가 목록에 없으면 전체 반환 (유실 없이 safe)."""
    state = EventState(state_file=str(tmp_path / "state.json"))
    state.last_event_id = "999"
    events = [{"id": "4"}, {"id": "3"}]
    assert state.filter_new_events(events) == events


def test_filter_new_events_empty(tmp_path):
    """빈 이벤트 목록은 빈 리스트 반환."""
    state = EventState(state_file=str(tmp_path / "state.json"))
    state.last_event_id = "1"
    assert state.filter_new_events([]) == []


def test_state_persistence(tmp_path):
    """etag, last_event_id가 파일에 저장되고 재로드 후에도 유지된다."""
    path = str(tmp_path / "state.json")
    state = EventState(state_file=path)
    state.etag = "abc123"
    state.last_event_id = "42"

    reloaded = EventState(state_file=path)
    assert reloaded.etag == "abc123"
    assert reloaded.last_event_id == "42"


def test_state_defaults_on_missing_file(tmp_path):
    """상태 파일이 없으면 None 으로 초기화."""
    state = EventState(state_file=str(tmp_path / "nonexistent.json"))
    assert state.etag is None
    assert state.last_event_id is None
