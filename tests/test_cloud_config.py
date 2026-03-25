from core.cloud_config import load_cloud_config


def test_load_cloud_config_reads_supabase_env(monkeypatch) -> None:
    monkeypatch.setenv("HW_CLOUD_BACKEND", "supabase")
    monkeypatch.setenv("SUPABASE_URL", "https://demo.supabase.co")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "service-role-key")
    monkeypatch.setenv("SUPABASE_STORAGE_BUCKET", "hw-data")
    monkeypatch.setenv("SUPABASE_STORAGE_PREFIX", "prod")

    config = load_cloud_config()

    assert config.backend == "supabase"
    assert config.enabled is True
    assert config.is_configured is True
    assert config.project_url == "https://demo.supabase.co"
    assert config.service_role_key == "service-role-key"
    assert config.storage_bucket == "hw-data"
    assert config.storage_prefix == "prod"
