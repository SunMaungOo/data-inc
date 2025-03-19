
# Introduction

In Azure platform , the data flow lineage can by Azure Data Factory or Azure Synapse (which use the same syntax to represent pipeline)  

The data-flow syntax can be represent by the activity's ``User properties`` with the ``key`` that user choose.  

The thing to consider is that the ``key`` that we have choosen in the ``User properties`` must be consists in the data factory/synapse.

# Pipeline 

The azure platform allowed the pipline to be represented in hierarchical structure.  

Therefore we must have the ``full pipeline`` name which represent both pipeline name and the hierarchical structure it is in.  

The recommended representation are ``hello/foo`` to represent the pipeline called ``foo`` in the folder called ``hello`` and the ``/world`` to represent the pipeline called ``world`` which is not in any  hierarchical structure

# Loader

It is recommended to implements the loader components represents by ``Copy Activity user properties``.

# Transformer

It is recommended to implements the transformer components by the ``Store Procedure Activity's user properties`` or the ``Sql Pool Stored Procedure Activity's user properties``.  

It is also recommended to have same kind of implementation which that the user properties value and return the ``vih`` syntax.

# Caller

For the caller component,it is recommended to have the ``callerType`` called ``pipeline`` and the pipeline full name in the ``Execute Pipeline Activity's user properties``.  

For example ``call:pipeline|foo/world`` should be in the ``Execute Pipeline Activity's user properties`` to represent that it called the pipeline titled ``foo/world``

# Ignore Activity

There are activity in the data factory/synapse which does not have any impact on the data flow diagram such as ``Set Variable activity``.  

For those ``activity`` , it must not include the ``key`` that we have choosen in user properties to indicate that the azure interpreter should skip that activity.

# Nested Activity

For activity such as the ``If Activity`` and ``While Activity`` which can have nested activity , the user properties of that activity must not have ``key``.  

The activity which is nested needed to implements the data flow syntax as usual.  

The azure interpreter must replace that nested activity with the activity it contains.