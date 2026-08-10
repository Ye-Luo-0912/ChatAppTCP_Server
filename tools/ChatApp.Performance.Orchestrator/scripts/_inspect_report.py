import json, sys, glob

def load_latest_reports(root):
    reports = []
    for f in sorted(glob.glob(root + '/**/benchmark-report.json', recursive=True)):
        try:
            with open(f) as fh:
                reports.append(json.load(fh))
        except Exception as e:
            print('ERR', f, e)
    return reports

def inspect_summary(path):
    with open(path) as fh:
        d = json.load(fh)
    print('MEMPROF OverallSucceeded =', d.get('OverallSucceeded'))
    print('MEMPROF Profiles =', d.get('Profiles'))
    for r in (d.get('Results') or []):
        print('RESULT', r.get('Profile'), 'repeat=', r.get('Repeat'),
              'RunValid=', r.get('RunValid'), 'exit=', r.get('CapacityExitCode'),
              'err=', r.get('InvocationError'), 'gcd=', len(r.get('Gcdumps') or []),
              'socket=', (r.get('SocketSnapshot') is not None))

if __name__ == '__main__':
    root = sys.argv[1]
    if root.endswith('.json') and 'memory-profile-report' in root:
        inspect_summary(root)
        sys.exit(0)
    if root.endswith('.json') and 'tcp-load-' in root:
        with open(root) as fh:
            d = json.load(fh)
        print('KEYS =', list(d.keys()))
        for k in ('RuntimeFailure','ErrorSamples','Gate','FailedConnections','ServerClosed','ProtocolRejected','ChatSendFailed','ChatReceiveFailed','DeliveryDrainCompleted','DeliveryDrainElapsedSeconds','CompletedNormally','TotalElapsedSeconds','Healthy','Slow','Rejected','DuplicateDeliveries'):
            if k in d: print(k, '=', (d[k] if not isinstance(d[k],(list,dict)) else json.dumps(d[k])[:1500]))
        sys.exit(0)
    reports = load_latest_reports(root)
    for d in reports:
        print('=== report keys:', list(d.keys()))
        print('Succeeded =', d.get('Succeeded'))
        print('Validity =', d.get('Validity'))
        print('Errors =', d.get('Errors'))
        lr = d.get('LoadResults')
        if isinstance(lr, dict):
            for k, v in lr.items():
                if isinstance(v, dict):
                    print('  LoadResult', k, {kk: vv for kk, vv in v.items() if kk in ('TcpConnectionsAttempted','TcpConnectionsSucceeded','TcpConnectionsFailed','ConnectionsSucceeded','ConnectionsFailed','ConnectionsAttempted','AckRatio','AcknowledgementRatio','MeasurementSeconds','RunValid')})
        elif isinstance(lr, list):
            for v in lr:
                if isinstance(v, dict):
                    print('  LoadResult', {kk: vv for kk, vv in v.items() if 'onnection' in kk or 'ou' in kk or 'ack' in kk.lower()})
        gr = d.get('ProcessResources') or []
        labels = [r.get('Label') for r in gr]
        print('ProcessLabels =', labels)
        for r in gr:
            if str(r.get('Label','')).startswith('gateway'):
                print('  ', r.get('Label'),
                      'PSSmib=', round((r.get('MaximumPssBytes') or 0)/1048576, 2),
                      'VmRSSmib=', round((r.get('MaximumVmRssBytes') or 0)/1048576, 2),
                      'VmHWMmib=', round((r.get('MaximumVmHwmBytes') or 0)/1048576, 2),
                      'cgroupPeakmib=', round((r.get('MaximumCgroupMemoryPeakBytes') or 0)/1048576, 2),
                      'maxfd=', r.get('MaximumFileDescriptorCount'))