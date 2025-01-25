0. The goal of phase 2b is to perform benchmarking/scalability tests of sample three-tier lakehouse solution.

1. In main.tf, change machine_type at:

```
module "dataproc" {
  depends_on   = [module.vpc]
  source       = "github.com/bdg-tbd/tbd-workshop-1.git?ref=v1.0.36/modules/dataproc"
  project_name = var.project_name
  region       = var.region
  subnet       = module.vpc.subnets[local.notebook_subnet_id].id
  machine_type = "e2-standard-2"
}
```

and substitute "e2-standard-2" with "e2-standard-4".

  DONE in:
  [d6fa720](https://github.com/karolstepanienko/tbd-workshop-1/commit/d6fa720c4a2fd6991b32c6f338b775a7d031099f)

  For this test Composer is not necessary. In order to save costs it was removed from terraform configuration.

  TODO commit

2. If needed request to increase cpu quotas (e.g. to 30 CPUs): 
https://console.cloud.google.com/apis/api/compute.googleapis.com/quotas?project=tbd-2023z-9918

  DONE:
  ![quota.png](doc/figures/phase2/quota.png)

  Increasing CPU quotas is necessary only sometimes necessary due to Composer v2 using GKE autopilot. Number of CPU cores in that K8s cluster can sometimes be larger than necessary, which hits the quota.

3. Using tbd-tpc-di notebook perform dbt run with different number of executors, i.e., 1, 2, and 5, by changing:
```
 "spark.executor.instances": "2"
```

in profiles.yml.


  DONE comments:

  'Load staging' phase has to be performed before each test, otherwise dbt run will fail.

  This step calls only for changing the number of spark executor instances, but much better results can be achieved if the number of dataproc worker nodes is also increased alongside the number of spark executors. Because of this one additional test was performed with 5 dataproc worker nodes and a matching amount of spark executor instances.

4. In the notebook, collect console output from dbt run, then parse it and retrieve total execution time and execution times of processing each model. Save the results from each number of executors.

TODO test results in a table

5. Analyze the performance and scalability of execution times of each model. Visualize and discuss the final results.

TODO graph total time per number of executors
TODO graph db times per number of executors
