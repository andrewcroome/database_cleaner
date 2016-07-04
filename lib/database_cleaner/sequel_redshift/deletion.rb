require 'database_cleaner/sequel_redshift/base'
require 'database_cleaner/generic/truncation'
require 'database_cleaner/sequel_redshift/truncation'

module DatabaseCleaner::SequelRedshift
  class Deletion < Truncation
    def delete_tables(db, tables)
      tables.each do |table|
        db[table.to_sym].delete
      end
    end

    def clean
      return unless dirty?

      tables = tables_to_truncate(db)
      db.transaction do
        disable_referential_integrity(tables) do
          delete_tables(db, tables)
        end
      end
    end
  end
end
