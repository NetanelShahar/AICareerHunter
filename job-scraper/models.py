from dataclasses import dataclass, asdict


@dataclass
class Job:
    title: str
    company: str
    location: str
    date_posted: str
    source: str
    url: str
    keyword_matched: str

    def dedup_key(self) -> str:
        return f"{self.title.lower().strip()}|{self.company.lower().strip()}"

    def to_dict(self) -> dict:
        return asdict(self)
