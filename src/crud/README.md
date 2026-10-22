# crud/

`CrudService<R>`, a thin service over any `R: Repository`. It contains no
resource-specific code: a resource that needs bespoke query logic adds it in
its own controller against its own repository, not by extending the service.

Filter/sort/bulk support that is mechanically derivable from a resource's
field table (a numeric field always gets `eq`/`min`/`max`/`in`, a string
field `eq`/`contains`/`icontains`) belongs here or in `repositories::filtering`
rather than being re-derived per resource. `update_many` and `delete_many`
add an `owner_id` equality clause before delegating, the same ownership
scoping `update` and `delete` apply.

`DEFAULT_LIMIT` and `MAX_LIMIT` are the only other policy this module owns;
everything else passes through to the repository.
