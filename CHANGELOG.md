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



