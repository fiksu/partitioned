module Partitioned
  class PartitionedBase
    module Configurator
      # coalesces and parses all {Data} objects allowing the
      # {PartitionManager} to request partitioning information froma
      # centralized source.
      class Reader
        attr_reader :model

        def initialize(most_derived_activerecord_class)
          @model = most_derived_activerecord_class
          @configurators = nil
          @on_fields = nil
          @indexes = nil
          @foreign_keys = nil
          @check_constraint = nil

          @schema_name = nil
          @name_prefix = nil
          @base_name = nil
          @part_name = nil

          @table_name = nil

          @parent_table_schema_name = nil
          @parent_table_name = nil

          @encoded_name = nil

          @janitorial_creates_needed = nil
          @janitorial_archives_needed = nil
          @janitorial_drops_needed = nil
          @after_partition_table_create_hooks = nil
        end

        #
        # The name of the schema that will contain all child tables.
        #
        def schema_name
          unless @schema_name
            @schema_name = collect_first(&:schema_name)
          end
          return @schema_name
        end

        #
        # The field used to partition child tables.
        #
        def on_fields
          unless @on_fields
            @on_fields = collect(&:on_field).map(&:to_sym)
          end
          return @on_fields
        end

        #
        # Define an index to be created on all (leaf-) child tables.
        #
        def indexes(*partition_key_values)
          return collect_from_collection(*partition_key_values, &:indexes).inject({}) do |bag, data_index|
            bag[data_index.field] = (data_index.options || {}) unless data_index.blank?
            bag
          end
        end

        #
        # Define a foreign key on a (leaf-) child table.
        #
        def foreign_keys(*partition_key_values)
          return collect_from_collection(*partition_key_values, &:foreign_keys).inject(Set.new) do |set,new_items|
            if new_items.is_a? Array
              set += new_items
            else
              set += [new_items]
            end
            set
          end
        end

        #
        # Define the check constraint for a given child table.
        #
        def check_constraint(*partition_key_values)
          return collect_first(*partition_key_values, &:check_constraint)
        end

        #
        # The table name of the table who is the direct ancestor of a child table.
        #
        def parent_table_name(*partition_key_values)
          return collect_first(*partition_key_values, &:parent_table_name)
        end

        #
        # The schema name of the table who is the direct ancestor of a child table.
        #
        def parent_table_schema_name(*partition_key_values)
          return collect_first(*partition_key_values, &:parent_table_schema_name)
        end

        #
        # The full name of a child table defined by the partition key values.
        #
        def table_name(*partition_key_values)
          if partition_key_values.length < 1
            return model.table_name
          else
            return collect_first(*partition_key_values, &:table_name)
          end
        end

        #
        # A reasonable alias for this partition table
        #
        def table_alias_name(*partition_key_values)
          return collect_first(*partition_key_values, &:table_alias_name)
        end

        #
        # The name of the child table without the schema name or name prefix.
        #
        def base_name(*partition_key_values)
          return collect_first(*partition_key_values, &:base_name)
        end

        #
        # The prefix for the child table's name.
        #
        def name_prefix
          unless @name_prefix
            @name_prefix = collect_first(&:name_prefix)
          end
          return @name_prefix
        end

        #
        # The child tables name without the schema name.
        #
        def part_name(*partition_key_values)
          return collect_first(*partition_key_values, &:part_name)
        end

        #
        # Define the order by clause used to list all child table names in order
        # of "last to be used" to "oldest to have been used".
        #
        def last_partitions_order_by_clause
          unless @last_partitions_order_by_clause
            @last_partitions_order_by_clause = collect_first(&:last_partitions_order_by_clause)
          end
          return @last_partitions_order_by_clause
        end

        def janitorial_creates_needed
          unless @janitorial_creates_needed
            @janitorial_creates_needed = collect_first(&:janitorial_creates_needed)
          end
          return @janitorial_creates_needed
        end

        def janitorial_archives_needed
          unless @janitorial_archives_needed
            @janitorial_archives_needed = collect_first(&:janitorial_archives_needed)
          end
          return @janitorial_archives_needed
        end

        def janitorial_drops_needed
          unless @janitorial_drops_needed
            @janitorial_drops_needed = collect_first(&:janitorial_drops_needed)
          end
          return @janitorial_drops_needed
        end

        def run_after_partition_table_create_hooks(*partition_key_values)
          collect_from_collection(*partition_key_values, &:after_partition_table_create_hooks)
        end

        protected

        def configurators
          unless @configurators
            @configurators = []
            model.ancestors.each do |ancestor|
              if ancestor.respond_to?(:configurator_dsl)
                if ancestor::configurator_dsl
                  @configurators << ancestor::configurator_dsl
                end
              end
              break if ancestor == Partitioned::PartitionedBase
            end
          end
          return @configurators
        end

        def collect(*partition_key_values, &block)
          values = []
          configurators.each do |configurator|
            data = configurator.data

            intermediate_value = block.call(data) rescue nil
            if intermediate_value.is_a? Proc
              values << intermediate_value.call(model, *partition_key_values)
            elsif intermediate_value.is_a? String
              field_value = partition_key_values.first
              values << eval("\"#{intermediate_value}\"")
            else
              values << intermediate_value unless intermediate_value.blank?
            end
          end
          return values
        end

        def collect_first(*partition_key_values, &block)
          configurators.each do |configurator|
            data = configurator.data
            intermediate_value = block.call(data) rescue nil
            if intermediate_value.is_a? Proc
              return intermediate_value.call(model, *partition_key_values)
            elsif intermediate_value.is_a? String
              field_value = partition_key_values.first
              return eval("\"#{intermediate_value}\"")
            else
              return intermediate_value unless intermediate_value.nil?
            end
          end
          return nil
        end

        def collect_from_collection(*partition_key_values, &block)
          values = []
          configurators.each do |configurator|
            data = configurator.data
            intermediate_values = []
            intermediate_values = block.call(data) rescue nil
            [*intermediate_values].each do |intermediate_value|
              if intermediate_value.is_a? Proc
                values << intermediate_value.call(model, *partition_key_values)
              elsif intermediate_value.is_a? String
                field_value = partition_key_values.first
                values << eval("\"#{intermediate_value}\"")
              else
                values << intermediate_value unless intermediate_value.blank?
              end
            end
          end
          return values
        end
      end
    end
  end
end
