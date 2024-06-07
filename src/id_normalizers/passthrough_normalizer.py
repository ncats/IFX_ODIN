from typing import Union, List, Dict

from src.interfaces.id_normalizer import IdNormalizer, IdNormalizerResult, NormalizationMatch


class PassthroughNormalizer(IdNormalizer):
    def normalize(self, input_ids: Union[str, List[str]]) -> Dict[str, IdNormalizerResult]:
        return {input_id: IdNormalizerResult(
            best_matches=[NormalizationMatch(match=input_id, context=['exact'])],
            other_matches=[]
        ) for input_id in input_ids}
