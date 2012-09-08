require 'active_record/base'

module ActiveRecord
  class Base
    def arel_table
      symbolized_attributes = attributes.symbolize_keys
      table_partition_key_values = Hash[*self.class.table_partition_keys.map{|name| [name,symbolized_attributes[name]]}.flatten]
      return self.class.arel_table(table_partition_key_values)
    end

    class << self # Class methods
      def reset_arel_table(arel_attribute_values = {})
        @arel_tables ||= {}
        if arel_attribute_values.blank?
          key_values = nil
        else
          key_values = self.table_partition_key_values(arel_attribute_values)
        end
        @arel_tables[key_values] = nil
      end

      def arel_table(arel_attribute_values = {})
        @arel_tables ||= {}

        if arel_attribute_values.blank?
          key_values = nil
        else
          key_values = self.table_partition_key_values(arel_attribute_values)
        end
        new_arel_table = @arel_tables[key_values]
        if new_arel_table.blank?
          new_arel_table = Arel::Table.new(table_name(*key_values), arel_engine)
          @arel_tables[key_values] = new_arel_table
        end
        return new_arel_table
      end
    end
  end
end
