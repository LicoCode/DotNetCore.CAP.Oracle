﻿// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using DotNetCore.CAP.Internal;
using DotNetCore.CAP.Persistence;
using DotNetCore.CAP.Oracle;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

// ReSharper disable once CheckNamespace
namespace DotNetCore.CAP;

internal class OracleCapOptionsExtension : ICapOptionsExtension
{
    private readonly Action<OracleOptions> _configure;

    public OracleCapOptionsExtension(Action<OracleOptions> configure)
    {
        _configure = configure;
    }

    public void AddServices(IServiceCollection services)
    {
        services.AddSingleton(new CapStorageMarkerService("Oracle"));
        services.AddSingleton<IDataStorage, OracleDataStorage>();
        services.AddSingleton<IStorageInitializer, OracleStorageInitializer>();
        services.Configure(_configure);
        services.AddSingleton<IConfigureOptions<OracleOptions>, ConfigureOracleOptions>();
    }
}