import datetime
import time
from dataclasses import dataclass, field
from typing import List, Dict
import humanize

from src.interfaces.id_resolver import IdResolver
from src.interfaces.input_adapter import InputAdapter
from src.interfaces.output_adapter import OutputAdapter


@dataclass
class ETL:
    input_adapters: List[InputAdapter]
    output_adapters: List[OutputAdapter]
    resolver_map: Dict[str, IdResolver] = field(default_factory=dict)

    def create_or_truncate_datastores(self, truncate_tables: bool = None):
        for output_adapter in self.output_adapters:
            if not output_adapter.create_or_truncate_datastore(truncate_tables=truncate_tables):
                raise Exception("operation cancelled")

    def do_etl(self, do_post_processing = True, clean_edges: bool = True, resume: bool = False,
               run_id: str | None = None):
        total_start_time = time.time()

        completed_adapters = set()
        if run_id:
            if resume:
                for output_adapter in self.output_adapters:
                    completed_adapters |= output_adapter.get_completed_adapter_names(run_id)
                if completed_adapters:
                    print(f"Resume mode: skipping {len(completed_adapters)} completed adapters")
            else:
                for output_adapter in self.output_adapters:
                    output_adapter.reset_run_state(run_id)

        for output_adapter in self.output_adapters:
            output_adapter.do_pre_processing()

        adapter_total = len(self.input_adapters)
        for adapter_position, input_adapter in enumerate(self.input_adapters, start=1):
            adapter_name = input_adapter.get_name()
            if resume and adapter_name in completed_adapters:
                print(f"Skipping completed adapter [{adapter_position}/{adapter_total}]: {adapter_name}")
                continue

            start_time = time.time()
            print(f"Running [{adapter_position}/{adapter_total}]: {adapter_name}")
            if run_id:
                for output_adapter in self.output_adapters:
                    output_adapter.mark_adapter_running(
                        run_id=run_id,
                        adapter_name=adapter_name,
                        adapter_position=adapter_position,
                        adapter_total=adapter_total,
                    )
            count = 0
            try:
                for resolved_list in input_adapter.get_resolved_and_provenanced_list(resolver_map = self.resolver_map):
                    count += len(resolved_list)
                    for output_adapter in self.output_adapters:
                        resolved_list = output_adapter.preprocess_objects(resolved_list)
                        output_adapter.store(resolved_list, single_source=input_adapter.is_single_source())
            except Exception as exc:
                if run_id:
                    for output_adapter in self.output_adapters:
                        output_adapter.mark_adapter_failed(
                            run_id=run_id,
                            adapter_name=adapter_name,
                            error_message=str(exc),
                            adapter_position=adapter_position,
                            adapter_total=adapter_total,
                        )
                raise

            end_time = time.time()
            elapsed_time = end_time - start_time
            print(f"\tElapsed time: {elapsed_time:.4f} seconds merging {count} records")
            if run_id:
                for output_adapter in self.output_adapters:
                    output_adapter.flush_incremental_metadata()
                    output_adapter.mark_adapter_completed(
                        run_id=run_id,
                        adapter_name=adapter_name,
                        records_written=count,
                        adapter_position=adapter_position,
                        adapter_total=adapter_total,
                    )

        if do_post_processing:
            for output_adapter in self.output_adapters:
                output_adapter.do_post_processing(clean_edges=clean_edges)

        total_elapsed_time = time.time() - total_start_time
        elapsed_timedelta = datetime.timedelta(seconds=total_elapsed_time)

        # Format the elapsed time using humanize
        formatted_time = humanize.precisedelta(elapsed_timedelta, format='%0.0f')

        print(f"\tTotal elapsed time: {formatted_time}")
