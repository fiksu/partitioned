require 'active_record/connection_adapters/abstract/schema_definitions'

module ActiveRecord
  module ConnectionAdapters #:nodoc:
    class TableDefinition
      #
      # Builds a SQL check constraint
      #
      # @param [String] constraint a SQL constraint
      def check_constraint(constraint)
        @columns << Struct.new(:to_sql).new("CHECK (#{constraint})")
      end
    end
  end
end
