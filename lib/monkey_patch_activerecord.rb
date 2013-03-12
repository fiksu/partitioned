require 'active_record'
require 'active_record/base'
require 'active_record/connection_adapters/abstract_adapter'
require 'active_record/relation.rb'
require 'active_record/persistence.rb'

#
# Patching {ActiveRecord} to allow specifying the table name as a function of
# attributes.
#
module ActiveRecord
  #
  # Patches for Persistence to allow certain partitioning (that related to the primary key) to work.
  #
  module Persistence
    # Deletes the record in the database and freezes this instance to reflect
    # that no changes should be made (since they can't be persisted).
    def destroy
      destroy_associations

      if persisted?
        IdentityMap.remove(self) if IdentityMap.enabled?
        pk         = self.class.primary_key
        column     = self.class.columns_hash[pk]
        substitute = connection.substitute_at(column, 0)

        using_arel_table = self.respond_to?(:dynamic_arel_table) ? dynamic_arel_table() : self.class.arel_table
        relation = self.class.unscoped.where(
          using_arel_table[pk].eq(substitute))

        relation.bind_values = [[column, id]]
        relation.delete_all
      end

      @destroyed = true
      freeze
    end

    # Updates the associated record with values matching those of the instance attributes.
    # Returns the number of affected rows.
    def update(attribute_names = @attributes.keys)
      attributes_with_values = arel_attributes_values(false, false, attribute_names)
      return 0 if attributes_with_values.empty?
      klass = self.class
      using_arel_table = self.respond_to?(:dynamic_arel_table) ? dynamic_arel_table() : klass.arel_table
      stmt = klass.unscoped.where(using_arel_table[klass.primary_key].eq(id)).arel.compile_update(attributes_with_values)
      klass.connection.update stmt
    end

    #
    # patch the create method to prefetch the primary key if needed
    #
    def create
      if self.id.nil? && self.class.respond_to?(:prefetch_primary_key?) && self.class.prefetch_primary_key?
        self.id = connection.next_sequence_value(self.class.sequence_name)
      end

      attributes_values = arel_attributes_values(!id.nil?)

      new_id = self.class.unscoped.insert attributes_values

      self.id ||= new_id

      IdentityMap.add(self) if IdentityMap.enabled?
      @new_record = false
      id
    end
  end
  #
  # Patches for relation to allow back hooks into the {ActiveRecord}
  # requesting name of table as a function of attributes.
  #
  class Relation
    #
    # Patches {ActiveRecord}'s building of an insert statement to request
    # of the model a table name with respect to attribute values being
    # inserted.
    #
    # The differences between this and the original code are small and marked
    # with PARTITIONED comment.
    def insert(values)
      primary_key_value = nil

      if primary_key && Hash === values
        primary_key_value = values[values.keys.find { |k|
          k.name == primary_key
        }]

        if !primary_key_value && connection.prefetch_primary_key?(klass.table_name)
          primary_key_value = connection.next_sequence_value(klass.sequence_name)
          values[klass.arel_table[klass.primary_key]] = primary_key_value
        end
      end

      im = arel.create_insert
      #
      # PARTITIONED ADDITION. get arel_table from class with respect to the
      # current values to placed in the table (which hopefully hold the values
      # that are used to determine the child table this insert should be
      # redirected to)
      #
      actual_arel_table = @klass.dynamic_arel_table(Hash[*values.map{|k,v| [k.name,v]}.flatten]) if @klass.respond_to?(:dynamic_arel_table)
      actual_arel_table = @table unless actual_arel_table
      im.into actual_arel_table

      conn = @klass.connection

      substitutes = values.sort_by { |arel_attr,_| arel_attr.name }
      binds       = substitutes.map do |arel_attr, value|
        [@klass.columns_hash[arel_attr.name], value]
      end

      substitutes.each_with_index do |tuple, i|
        tuple[1] = conn.substitute_at(binds[i][0], i)
      end

      if values.empty? # empty insert
        im.values = Arel.sql(connection.empty_insert_statement_value)
      else
        im.insert substitutes
      end

      conn.insert(
        im,
        'SQL',
        primary_key,
        primary_key_value,
        nil,
        binds)
    end
  end
end
