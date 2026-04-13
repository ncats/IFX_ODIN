from dataclasses import dataclass, field
from typing import Optional, Dict


@dataclass
class WorkbookArtifact:
    file_reference: Optional[str] = None
    original_filename: Optional[str] = None
    media_type: Optional[str] = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    _local_path: Optional[str] = field(default=None, repr=False)

    def to_dict(self) -> Dict[str, Optional[str]]:
        return {
            "file_reference": self.file_reference,
            "original_filename": self.original_filename,
            "media_type": self.media_type,
        }

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            file_reference=data.get("file_reference"),
            original_filename=data.get("original_filename"),
            media_type=data.get("media_type"),
        )
