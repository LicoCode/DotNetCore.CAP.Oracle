// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;

// ReSharper disable once CheckNamespace
namespace DotNetCore.CAP;

public class EFOptions
{
    public const string DefaultTablePrefix = "cap";

    /// <summary>
    /// Gets or sets the TablePrefix to use when creating database objects.
    /// Default is <see cref="DefaultTablePrefix" />.
    /// </summary>
    public string TablePrefix { get; set; } = DefaultTablePrefix;

    /// <summary>
    /// EF DbContext
    /// </summary>
    internal Type? DbContextType { get; set; }

    /// <summary>
    /// Data version
    /// </summary>
    internal string Version { get; set; } = default!;
}