"""Rewrites the compose Prometheus scrape jobs for k3s (filter `pc_k3s_scrape`)."""
import re

import yaml


def _host(target, namespace, renames):
    """service:port or scheme://service:port/path -> the namespaced Service name."""
    target = renames.get(target, target)
    m = re.match(r'^(?P<scheme>[a-z]+://)?(?P<host>[A-Za-z0-9.-]+)(?P<rest>:.*)$', target)
    if not m or '.' in m.group('host'):
        return target
    host = renames.get(m.group('host'), m.group('host'))
    return (m.group('scheme') or '') + host + '.' + namespace + m.group('rest')


def pc_k3s_scrape(text, namespace, drop_jobs, renames=None):
    """Drops the jobs kube-prometheus-stack scrapes itself and points the rest at
    <service>.<namespace>, since Prometheus runs in another namespace. Job names stay."""
    renames = renames or {}
    cfg = yaml.safe_load(text)
    jobs = []
    for job in cfg.get('scrape_configs', []):
        if job.get('job_name') in drop_jobs:
            continue
        for sc in job.get('static_configs', []):
            sc['targets'] = [_host(t, namespace, renames) for t in sc.get('targets', [])]
        for rc in job.get('relabel_configs', []):
            if rc.get('target_label') == '__address__' and 'replacement' in rc:
                rc['replacement'] = _host(rc['replacement'], namespace, renames)
        jobs.append(job)
    return yaml.safe_dump(jobs, default_flow_style=False, sort_keys=False)


class FilterModule(object):
    def filters(self):
        return {'pc_k3s_scrape': pc_k3s_scrape}
