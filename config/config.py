import yaml
yaml_file = 'resource/application.yaml'
with open(yaml_file, 'r') as yml:
    cfg = yaml.safe_load(yml)
# 설정 파일 로드 클래스
class TargetConfig:
    DRUID_INGESTION_URL = cfg['DRUID']['URL']['INGESTION']
    DRUID_DELETE_URL = cfg['DRUID']['URL']['DELETE']
