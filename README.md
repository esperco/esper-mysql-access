This repository was extracted from a larger internal project at
[Esper](https://esper.com).
We released it in the hope that it might be useful to other
OCaml developers.

It won't build as is but most of the code was used in production.

Description
-----------

OCaml/Lwt support for managing a MySQL database as an enhanced key-value 
store. Three table schemas are supported:

* KV: (key, value, float)
* KKV: (non-unique key, unique key, value, float)
* K2V: (key1, key2, value, float), where the pair (key1, key2) is unique

The KV schema is used when a simple key-value mapping is sufficient. 
Additionally, the last column `ord` is a float that can be used to order the 
records in an arbitrary fashion but typically chronologically.

The KKV schema serves the sames purpose as the KV schema but provides an 
additional indexed column in the form a non-unique key. For example this 
non-unique key may be a user ID while the unique key may be an item owned by 
the user. It makes it possible to retrieve all the items of that user.

The K2V schema allows arbitrary relations between two sets of objects. For 
example, `k1` may be a group ID and `k2` a user ID. It's possible to check 
whether the user belongs to a particular group by querying a specific pair
(k1, k2). It's also possible to list all the users in a specific group `k1`
or all the groups of which user `k2` is a member.

Even though the MySQL table schemas are generic, access to a specific table 
is type-safe. In order to create an interface to a table, the user must 
provide the table name, the type of the key and value fields, and functions to 
convert those fields from and to strings (or floats in the case of the `ord` 
column).
