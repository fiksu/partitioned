module Partitioned
  class MultiLevel
    module Configurator
      # coalesces and parses all {Data} objects allowing the
      # {PartitionManager} to request partitioning information froma
      # centralized source from multi level partitioned models
      class Reader < Partitioned::PartitionedBase::Configurator::Reader

        alias :super_schema_name :schema_name
        alias :super_parent_table_schema_name :parent_table_schema_name
        alias :super_parent_table_name :parent_table_name

        def initialize(most_derived_activerecord_class, primary_configurator=nil)
          super(most_derived_activerecord_class)
          @using_classes = nil
          @using_configurators = nil

          if primary_configurator
            @primary_configurator = primary_configurator
            @is_primary = false
          else
            @primary_configurator = self
            @is_primary = true
          end
          set_up_classes_and_configurators
        end

        #
        # The field used to partition child tables.
        #
        # @return [Array<Symbol>] fields used to partition this model
        def on_fields
          unless @on_fields
            @on_fields = []
            using_configurators.each do |configurator|
              @on_fields += configurator.collect(&:on_field).map(&:to_sym)
            end
          end
          return @on_fields
        end        

        def is_primary?
          return @is_primary
        end

        def primary_configurator
          return @primary_configurator
        end

        def schema_name
          primary_configurator.super_schema_name
        end

        #
        # The schema name of the table who is the direct ancestor of a child table.
        #
        def parent_table_schema_name(*partition_key_values)
          if partition_key_values.length <= 1
            return primary_configurator.super_parent_table_schema_name
          end
          return schema_name
        end
        
        def run_after_partition_table_create_hooks(*partition_key_values)
          using_configurators.each do |configurator|
            configurator.run_after_partition_table_create_hooks
          end
        end

        #
        # The table name of the table who is the direct ancestor of a child table.
        #
        def parent_table_name(*partition_key_values)
          if partition_key_values.length <= 1
            return primary_configurator.super_parent_table_name
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
          return "#{parent_table_schema_name(*partition_key_values)}.#{part_name(*partition_key_values[0...-1])}"
        end

        #
        # Define the check constraint for a given child table.
        #
        def check_constraint(*partition_key_values)
          index = partition_key_values.length-1
          value = partition_key_values[index]
          return using_configurator(index).check_constraint(value)
        end

        def child_partitions_order_type(*partition_key_values)
          index = partition_key_values.length
          return using_configurator(index).child_partitions_order_type(*partition_key_values)
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

        #
        # The key value corresponding to a specific base_name.
        #
        def key_value(base_name)
          parts = []
          base_name.split('_').each_with_index do |value,index|
            parts += using_configurator(index).key_value(value)
          end
          return parts
        end
        
        #
        # Indexes to create on each leaf partition.
        #
        def indexes(*partition_key_values)
          bag = {}
          using_configurators.each do |configurator|
            bag.merge!(configurator.indexes(*partition_key_values))
          end
          return bag
        end
        
        #
        # Foreign keys to create on each leaf partition.
        #
        def foreign_keys(*partition_key_values)
          set = Set.new
          using_configurators.each do |configurator|
            set.merge(configurator.foreign_keys(*partition_key_values))
          end
          return set
        end
        
        # retrieve a specific configurator from an ordered list.  for multi-level partitioning
        # we need to find the specific configurator for the partitioning level we are interested
        # in managing.
        #
        # @param [Integer] index the partitioning level to query
        # @return [Configurator] the configurator for the specific level queried
        def using_configurator(index)
          return using_configurators[index]
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

        def using_configurators
          if is_primary?
            return @using_configurators
          else
            return primary_configurator.using_configurators
          end
        end

        def using_classes
          if is_primary?
            return @using_classes
          else
            return primary_configurator.using_classes
          end
        end
       
        protected

        def set_up_classes_and_configurators
          if is_primary?
            abstract_classes = collect_from_collection(&:using_classes).inject([]) do |array,new_items|
              array += [*new_items]
            end.to_a             
            
            @using_classes = abstract_classes.map do |abstract_class| 
              partition_class = Class.new(abstract_class)
              partition.table_name = self.model.table_name
              partition_class.configurator = self.class.new(partition_class, self)
              partition_class
            end

            @using_configurators = @using_classes.map do |partition_class|
              partition_class::Configurator::Reader.new(partition_class)
            end

            @using_classes << self.model
            @using_configurators << Partitioned::PartitionedBase::Configurator::Reader.new(self.model)
          end
        end
      end
    end
  end
end
