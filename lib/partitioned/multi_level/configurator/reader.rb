module Partitioned
  class MultiLevel
    module Configurator
      # coalesces and parses all {Data} objects allowing the
      # {PartitionManager} to request partitioning information froma
      # centralized source from multi level partitioned models
      class Reader < Partitioned::PartitionedBase::Configurator::Reader
        
        alias :base_collect_from_collection :collect_from_collection
        alias :base_collect :collect
        
        # configurator for a specific class level
        UsingConfigurator = Struct.new(:model, :sliced_class, :dsl)

        def initialize(most_derived_activerecord_class)
          super
          @using_classes = nil
          @using_configurators = nil
        end

        #
        # The field used to partition child tables.
        #
        # @return [Array<Symbol>] fields used to partition this model
        def on_fields
          unless @on_fields
            @on_fields = collect(&:on_field).map(&:to_sym)
          end
          return @on_fields
        end

        #
        # The schema name of the table who is the direct ancestor of a child table.
        #
        def parent_table_schema_name(*partition_key_values)
          if partition_key_values.length <= 1
            return super
          end

          return schema_name
        end

        #
        # The table name of the table who is the direct ancestor of a child table.
        #
        def parent_table_name(*partition_key_values)
          if partition_key_values.length <= 1
            return super
          end

          # [0...-1] is here because the base name for this parent table is defined by the remove the leaf key value
          # that is:
          # current top level table name: public.foos
          # child schema area: foos_partitions
          # current partition classes: ByCompanyId then ByCreatedAt
          # current key values:
          #   company_id: 42
          #   created_at: 2011-01-03
          # child table name: foos_partitions.p42_20110103
          # parent table: foos_partitions.p42
          # grand parent table: public.foos
          return parent_table_schema_name(*partition_key_values) + '.p' + base_name(*partition_key_values[0...-1])
        end

        #
        # Define the check constraint for a given child table.
        #
        def check_constraint(*partition_key_values)
          index = partition_key_values.length - 1
          value = partition_key_values[index]
          return using_configurator(index).check_constraint(value)
        end

        def indexes(*partition_key_values)
          bag = {}
          partition_key_values.each_with_index do |key_value, index|
            bag.merge!(using_configurator(index).indexes(key_value))
          end
          base_collect_from_collection(*partition_key_values, &:indexes).inject(bag) do |bag, data_index|
            bag[data_index.field] = (data_index.options || {}) unless data_index.blank?
            bag
          end
        end
        
        #
        # Foreign keys to create on each leaf partition.
        #
        def foreign_keys(*partition_key_values)
          set = Set.new
          partition_key_values.each_with_index do |key_value, index|
            set.merge(using_configurator(index).foreign_keys(key_value))
          end
          base_collect_from_collection(*partition_key_values, &:foreign_keys).inject(set) do |set, new_items|
            if new_items.is_a? Array
              set += new_items
            else
              set += [new_items]
            end
            set
          end
        end

        #
        # The name of the child table without the schema name or name prefix.
        #
        def base_name(*partition_key_values)
          parts = []
          partition_key_values.each_with_index do |value,index|
            parts << using_configurator(index).base_name(value)
          end
          return parts.join('_')
        end

        # retrieve a specific configurator from an ordered list.  for multi-level partitioning
        # we need to find the specific configurator for the partitioning level we are interested
        # in managing.
        #
        # @param [Integer] index the partitioning level to query
        # @return [Configurator] the configurator for the specific level queried
        def using_configurator(index)
          return using_class(index).configurator
        end

        # retrieve a specific partitioning class from an ordered list.  for multi-level partitioning
        # we need to find the specific {Partitioned::PartitionedBase} class for the partitioning level we are interested
        # in managing.
        #
        # @param [Integer] index the partitioning level to query
        # @return [{Partitioned::PartitionedBase}] the class for the specific level queried
        def using_class(index)
          return using_classes[index]
        end


        protected

        def using_configurators
          unless @using_configurators
            @using_configurators = []
            using_classes.each do |using_class|
              using_class.ancestors.each do |ancestor|
                next if ancestor.class == Module
                @using_configurators << UsingConfigurator.new(using_class, ancestor, ancestor::configurator_dsl) if ancestor::configurator_dsl
                break if ancestor == Partitioned::PartitionedBase
              end
            end
          end
          return @using_configurators
        end

        def using_classes
          unless @using_classes
            @using_classes = base_collect_from_collection(&:using_classes).inject([]) do |array,new_items|
              array += [*new_items]
            end.to_a
          end
          return @using_classes
        end

        def collect(*partition_key_values, &block)
          values = []
          using_configurators.each do |using_configurator|
            data = using_configurator.dsl.data
            intermediate_value = block.call(data) rescue nil
            if intermediate_value.is_a? Proc
              values << intermediate_value.call(using_configurator.model, *partition_key_values)
            elsif intermediate_value.is_a? String
              values << eval("\"#{intermediate_value}\"")
            else
              values << intermediate_value unless intermediate_value.blank?
            end
          end
          return base_collect(*partition_key_values, &block) + values
        end

        def collect_from_collection(*partition_key_values, &block)
          values = []
          using_configurators.each do |using_configurator|
            data = using_configurator.dsl.data
            intermediate_values = []
            intermediate_values = block.call(data) rescue nil
            [*intermediate_values].each do |intermediate_value|
              if intermediate_value.is_a? Proc
                values << intermediate_value.call(using_configurator.model, *partition_key_values)
              elsif intermediate_value.is_a? String
                values << eval("\"#{intermediate_value}\"")
              else
                values << intermediate_value unless intermediate_value.blank?
              end
            end
          end
          return base_collect_from_collection(*partition_key_values, &block) + values
        end

      end
    end
  end
end
