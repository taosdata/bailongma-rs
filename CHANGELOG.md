<a name="v0.2.3"></a>
## v0.2.3 (2021-06-08)


#### Utils

* **blm-bench-prom:**  support table check in CI ([5c83a0d1](5c83a0d1))



<a name="v0.2.2"></a>
## v0.2.2 (2021-06-08)


#### Bug Fixes

* **remote-read:**  fix tag name not match for t_ prefix ([4dc58df3](4dc58df3))

#### Utils

*   add changelog section Utils ([4d86fedd](4d86fedd))
* **clog:**  add enhancement changelog section ([e96c5bac](e96c5bac))
* **prom-decode:**  support read/write request ([047396f1](047396f1))

#### Enhancement

* **prom:**  use different prefix for failed r/w requests ([bb3019c3](bb3019c3))



<a name="v0.2.1"></a>
## v0.2.1 (2021-06-07)


#### Bug Fixes

* **[TD-4607]:**  fix prom remote read results labels error ([c85a9cc8](c85a9cc8))

#### CI

*   add blm-bench-prom to release ([ad309aaa](ad309aaa))



<a name="v0.2.0"></a>
## v0.2.0 (2021-06-07)


#### Features

* **[TD-4607]:**  support regex match/not-match for promql ([e03be78a](e03be78a))

#### Bug Fixes

*   improve error log in case of prom data error ([4bf725f0](4bf725f0))



<a name="v0.1.1"></a>
## v0.1.1 (2021-06-04)


#### CI

*   use nightly toolchain to enable backtrace ([b6dd4e2b](b6dd4e2b))

#### Bug Fixes

*   fix sql syntax error if value contains quote ([645ed56a](645ed56a))
* **Prometheus:**  use escape_default for tag value ([a38d5041](a38d5041))



<a name="v0.1.0"></a>
## v0.1.0 (2021-06-02)


#### Bug Fixes

*   use actix arbiter instead of tokio spawn ([30a42b15](30a42b15))
*   set response status as Accepted (202) ([c1413326](c1413326))
*   max_connections not work ([7e87dfe4](7e87dfe4))
*   lowercase all tag names ([3602c492](3602c492))
*   remove rwlock ([bf3bd26f](bf3bd26f))
*   fix pool size too small ([5bfddb03](5bfddb03))
*   fix space lost in insert batch ([e131a888](e131a888))

#### Features

*   memory-efficient implements ([8d294c88](8d294c88))
*   use backtrace feature to compile in stable ([3d4d7a13](3d4d7a13))
*   support early reply to prometheus ([76570a2e](76570a2e))
*   add benchmark tool for prometheus ([cd368f1b](cd368f1b))
*   split rest feature to bailongma ([43fb09c1](43fb09c1))
*   add chunk size option to reduce error ([00e1d4d0](00e1d4d0))
*   use rest api for memory leak case ([10f8c55f](10f8c55f))
*   support config options in command line ([bed3d65b](bed3d65b))
* **CHANGELOG:**  auto generate CHANGELONG.md by clog-cli ([bbe4e75a](bbe4e75a))
* **[TD-4496]:**  add build step for bailongma ([9b132b3d](9b132b3d))



