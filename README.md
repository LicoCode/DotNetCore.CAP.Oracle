# DotNetCore.CAP.Oracle

```c#
# DotNetCore.CAP.Oracle

builder.Services.AddCap(op =>
{
    op.UseDashboard();
    op.UseStorageLock = true;
    op.FailedRetryCount = 3;
    op.UseOracle(x =>
    {
        x.TablePrefix = "CAP";
        x.ConnectionString = "user id=******;data source=localhost:1521/orcl;password=******;";
    });

});
```

