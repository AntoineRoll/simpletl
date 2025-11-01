import polars as pl
import logging

logging.basicConfig(level=logging.INFO)

class TransformationEngine:
    def apply_step(self, data, transformation_step):
        logging.info(f"Applying transformation step: {transformation_step}")
        step_type = transformation_step['type']
        if step_type == 'filter':
            condition = transformation_step['condition']
            return self.filter_data(data, condition)
        elif step_type == 'aggregate':
            group_by = transformation_step['group_by']
            operation = transformation_step['operation']
            return self.aggregate_data(data, group_by, operation)
        else:
            logging.error(f"Unsupported transformation type: {step_type}")
            raise ValueError(f"Unsupported transformation type: {step_type}")

    def filter_data(self, data, condition):
        logging.info(f"Filtering data with condition: {condition}")
        # Example: Assuming 'condition' is a string safe to evaluate
        # In a real-world scenario, use a safer evaluation method
        filtered_data = data.eval(condition)
        return filtered_data

    def aggregate_data(self, data, group_by, operation):
        logging.info(f"Aggregating data by {group_by} with operation {operation}")
        if operation == 'sum':
            aggregated_data = data.groupby(group_by).agg(pl.col(group_by).sum().alias(group_by))
        elif operation == 'mean':
            aggregated_data = data.groupby(group_by).agg(pl.col(group_by).mean().alias(group_by))
        elif operation == 'count':
            aggregated_data = data.groupby(group_by).agg(pl.col(group_by).count().alias(group_by))
        else:
            logging.error(f"Unsupported aggregation operation: {operation}")
            raise ValueError(f"Unsupported aggregation operation: {operation}")
        return aggregated_data