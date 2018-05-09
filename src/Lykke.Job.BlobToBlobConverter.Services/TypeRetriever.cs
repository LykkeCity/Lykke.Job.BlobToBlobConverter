using Common.Log;
using Lykke.Job.BlobToBlobConverter.Core.Services;
using NuGet.Configuration;
using NuGet.Packaging;
using NuGet.Protocol;
using NuGet.Protocol.Core.Types;
using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace Lykke.Job.BlobToBlobConverter.Services
{
    public partial class TypeRetriever : ITypeRetriever
    {
        private const string _libsDir = "libs";
        private const string _dllExtension = ".dll";

        private readonly PackageMetadataResource _packageMetadataResource;
        private readonly DownloadResource _downloadResource;
        private readonly PackageDownloadContext _packageDownloadContext;
        private readonly SourceCacheContext _sourceCacheContext;
        private readonly NugetLogger _nugetLogger;
        private readonly string _downloadDirectory;

        public TypeRetriever(ILog log)
        {
            List<Lazy<INuGetResourceProvider>> providers = new List<Lazy<INuGetResourceProvider>>();
            providers.AddRange(Repository.Provider.GetCoreV3());  // Add v3 API support
            var packageSource = new PackageSource("https://api.nuget.org/v3/index.json");
            SourceRepository sourceRepository = new SourceRepository(packageSource, providers);
            _packageMetadataResource = sourceRepository.GetResource<PackageMetadataResource>();
            _downloadResource = sourceRepository.GetResource<DownloadResource>();

            string workingDir = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            _downloadDirectory = Path.Combine(workingDir, _libsDir);
            _sourceCacheContext = new SourceCacheContext
            {
                NoCache = true,
                DirectDownload = true,
            };
            _packageDownloadContext = new PackageDownloadContext(
                _sourceCacheContext,
                _downloadDirectory,
                true);
            _nugetLogger = new NugetLogger(log);
        }

        public async Task<Type> RetrieveTypeAsync(string typeName, string nugetPackageName)
        {
            IEnumerable<IPackageSearchMetadata> searchMetadata = await _packageMetadataResource.GetMetadataAsync(
                nugetPackageName,
                false,
                false,
                _nugetLogger,
                CancellationToken.None);
            if (!searchMetadata.Any())
                throw new InvalidOperationException($"Package {nugetPackageName} not found");

            var packageInfo = searchMetadata
                .Cast<PackageSearchMetadata>()
                .OrderByDescending(p => p.Version)
                .First();

            var downloadResult = await _downloadResource.GetDownloadResourceResultAsync(
                packageInfo.Identity,
                _packageDownloadContext,
                null,
                _nugetLogger,
                CancellationToken.None);
            if (downloadResult.Status != DownloadResourceResultStatus.Available)
                throw new InvalidOperationException($"Nuget package {nugetPackageName} of version {packageInfo.Version} is not available for download");

            var pathResolver = new PackagePathResolver(_downloadDirectory);
            var extractContext = new PackageExtractionContext(_nugetLogger);
            var extractedFiles = PackageExtractor.ExtractPackage(downloadResult.PackageStream, pathResolver, extractContext, CancellationToken.None);

            var dllFiles = extractedFiles.Where(f => Path.GetExtension(f).ToLower() == _dllExtension);
            if (!dllFiles.Any())
                throw new InvalidOperationException($"Dll files not found in {_packageDownloadContext.DirectDownloadDirectory}");

            var assembly = Assembly.LoadFile(dllFiles.First());
            var type = assembly.GetType(typeName);
            if (type == null)
                throw new InvalidOperationException($"Type {typeName} not found among {assembly.ExportedTypes.Select(t => t.FullName).ToList().ToJson()}");
            return type;
        }
    }
}
