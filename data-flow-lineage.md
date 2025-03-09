
# Terms

I am going to use the term **data flow** to represent the job/pipeline/task in which data flows from one system to another and in a data project there can be multiple data flow.  

# Components

The data flow is comprised of  

| Component   | Definition  |
| ----------- | ----------- |
| Loader | In which data just flow from source to sink system without any transformation perform.Can be the SQL script.Think of it as pure data ingestion/Insert task|
| Transformer | In which data is transformed,deleted or the inserted into table|
| Caller | In which it is the data flow called external resource such as web service , data flow , --etc|

# Basic

In order to be platform neutral , the data flow lineage should only be represented in **text** format and should used the **interpreter** program take that **text** format as input and generate the data flow diagram.

# Syntax

The basic syntax of the data flow lineage is ``<component>:<component-value>`` , the component is one of ``load|transform|call``

# Loader

The component value of the loader is ``load:source:<sourceType>:<sourceValue>|target:<targetType>:targetValue``

The ``<sourceType>`` and ``<targetType>`` can only be 

1. ud (represent undefined)
2. blob (represent blob)
3. db (represent database)

The **ud** should be used when we wanted to ignore that field  

The **db** value will have the additional value such as ``<location>:tag[schema.table,schema.table]>``  
 
The ``<location>`` value can only be  

1. cloud (represent cloud database)
2. prime (represent on-premises datbase)

The ``tag`` value represent the database.The recommended tag should be in ``host.db`` or ``general format``.


The **interpreter** program should concat the tag and dot to form the complete table name reference.

```
Example

loader:source:ud:|target:db:cloud:myhost.mydb[s1.t1,s2.t2]

```

# Transformer

The component value of transformer is ``transform:db:<location>:tag[schema.procedure]``

The ``<location>`` value can only be  

1. cloud (represent cloud database)
2. prime (represent on-premises datbase)

The ``schema.procedure`` should represent the procedure of the transformer which transform the data.  

The transformer procedure should have the below syntax in the **text** format somewhere (recommended in comment section)

```
<vih>
 source:value1,value2|
 target:value3|;
 source:value4|
 target:value5;
</vih>
```

The source should represent the table that the data is used to transform.For example ``FROM|JOIN|Common Table Expression | Sub Query``.  

The target should represent the table the data was updated or inserted into.For example ``INSERT|UPDATE|INTO``.

It is recommended that the values should be in the ``schema.table`` format.


# Caller

The component value of caller is ``call:<callerType>|<callerValue>``  

The caller type should represent the type of external service we are calling from the caller since as the ``pipeline`` , ``external web service``.  

The caller value should represent the location of the external service such as the api.  

```
Example

call:api|https://api.my.service

```

# Limitation

Currently the ``db`` component type of ``loader`` component only support the ``single database source`` and ``single database target`` model.  

It does not support inserting table to the table from the multiple database source.


# Implementation

There may be other components which cannot be defined by ``Loader|Transformer|Caller`` such as setting the internal local variable in the data flow. Such task/components should be ignore by the **interpreter**.  

It would be helpful to assign the ``unique name`` to each component in the data flow in order to help generating the relationship of each component.  

Consider the above diagram ``component-1 --> component-2 --> component-3`` , if the component-2 is the component we do not handle , the final flow diagram will be ``component-1 --> component-3``. The interpreter should rearrange the flow diagram when needed.  

There could be two parallel component flow to the single component , the **interpreter** should also handle such case.







