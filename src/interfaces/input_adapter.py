from abc import ABC, abstractmethod
from typing import List

from src.interfaces.id_normalizer import IdNormalizer


class InputAdapter(ABC):
    name: str

    def next(self):
        entries = self.get_all()
        self.normalize_entries(entries)
        for entry in entries:
            yield entry

    @abstractmethod
    def normalize_entries(self, entries: List):
        raise NotImplementedError("derived classes must implement normalize_entries")

    @abstractmethod
    def get_all(self) -> List:
        raise NotImplementedError("derived classes must implement normalize_entries")


class NodeInputAdapter(InputAdapter, ABC):
    id_normalizer: IdNormalizer

    def normalize_entries(self, entries: List):
        id_map = self.id_normalizer.normalize([entry.id for entry in entries if hasattr(entry, 'id')])
        update_count, pass_count, unmatch_count, skip_count = 0, 0, 0, 0
        for entry in entries:
            if hasattr(entry, 'id') and entry.id in id_map:
                matches = id_map[entry.id]
                if matches.best_matches and len(matches.best_matches) > 0:
                    best_id = matches.best_matches[0].match
                    if best_id != entry.id:
                        # print(f"updating : {entry.id} to {best_id}")
                        entry.id = best_id
                        update_count += 1
                    else:
                        pass_count += 1
                else:
                    # print(f"no match: {entry.id}")
                    unmatch_count += 1
            else:
                skip_count += 1
        print(f"updating {update_count} ids, validating {pass_count} ids, passing {unmatch_count} ids")
        print(f"skipped: {skip_count}")



class RelationshipInputAdapter(InputAdapter, ABC):
    start_id_normalizer: IdNormalizer
    end_id_normalizer: IdNormalizer

    def normalize_entries(self, entries: List):
        pass
