> ### Archived
>
> **June 2020:** [Inara v2.3.0](https://github.com/jobseekerltd/inara/releases/tag/v2.3.0) now doesn't require partitioned. This project is now archived.

---

# Partitioned

> Table-based partitioning for ActiveRecord

**Partitioned** simulates partitions with multiple tables.

```rb
employee = Employee.create(name: 'Keith')
```

This would normally produce the following:

```sql
INSERT INTO employees ('name') values ('Keith');
```

With Partitioned, we see:

```sql
INSERT INTO employees_partitions.p1 ('name') values ('Keith');
```

For more information on how partitioned works, see:

- https://github.com/jobseekerltd/partitioned/blob/v2.1.0/README.md
- https://github.com/fiksu/partitioned/blob/v2.1.0/README.md

## Developer setup

Running tests:

```sh
cd docker
docker-compose up -d

# wait a bit and then
bundle exec rspec
```

