#!/usr/bin/env python3
from src.config.settings import get_settings


def main():
    s = get_settings()
    print("Environment:", s.environment)
    print("Debug:", s.debug)
    print("Version:", s.version)
    print("OpenAI key (LLMS):", s.llms.openai_api_key)
    print("Redis host:", s.redis.host)
    print("Postgres host:", s.postgres.host)
    print("Qdrant host:", s.qdrant.host)
    print("API host:", s.api.host)


if __name__ == '__main__':
    main()
