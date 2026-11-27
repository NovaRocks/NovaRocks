#!/usr/bin/env python3
"""Recompute v6 public-configuration arithmetic, without claiming measurement."""
import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MAX = (1 << 64) - 1


def checked(value):
    if type(value) is not int or not 0 <= value <= MAX:
        raise ValueError("Native transport arithmetic exceeds u64")
    return value


def add(*values):
    return checked(sum(checked(value) for value in values))


def mul(a, b):
    return checked(checked(a) * checked(b))


def root_encode_envelope(profile, root_process_limit_bytes):
    transport = profile['transport_support']
    root = profile['be_root_result']
    if root_process_limit_bytes is None:
        root_process_limit_bytes = root['joint_retained_bytes_per_process']
        limit_source = 'profile_default'
    else:
        limit_source = 'supplied_effective_limit'
    limit = checked(root_process_limit_bytes)
    fixed_charge = checked(root['fixed_schema_cursor_driver_capacity_bytes'])
    if not limit or not fixed_charge:
        raise ValueError("Root process limit and fixed charge must be positive")

    frontends = transport['authenticated_live_frontends_per_backend']
    send_holders = checked(root['live_send_holders'])
    roots = mul(frontends, transport['worst_case_roots_per_frontend_per_backend'])
    live_connections = mul(frontends, transport['connections_per_frontend_backend']['result'])
    live_streams = mul(live_connections, transport['streams_per_connection'])
    # Mirror the actual Root ingress admission, including its original tail terms.
    metadata = add(root['maximum_root_drivers'], send_holders, 1)
    ingress = add(live_streams, mul(roots, add(metadata, send_holders)))
    data = min(ingress, mul(send_holders, limit // fixed_charge))

    control_max = checked(profile['client_rows']['fixed_envelope_bytes'])
    data_max = checked(root['segment_backing_capacity_bytes'])
    data_increment = checked(data_max - control_max)
    return {
        'root_process_limit_source': limit_source,
        'root_process_limit_bytes': limit,
        'fixed_root_charge_bytes': fixed_charge,
        'ingress_owner_limit': ingress,
        'data_owner_limit': data,
        'control_frame_max_bytes': control_max,
        'data_frame_max_bytes': data_max,
        'encoding_requested_bytes': add(mul(ingress, control_max), mul(data, data_increment)),
        'profile_example_data_send_positions': mul(roots, send_holders),
    }


def envelope(profile, *, root_process_limit_bytes=None):
    t = profile['transport_support']
    tails = add(t['connecting_positions_per_lane'], t['closing_positions_per_lane'])
    per_peer = {
        lane: add(live, tails)
        for lane, live in t['connections_per_frontend_backend'].items()
    }
    per_peer.update({
        'membership': add(1, tails),
        'exchange': add(t['exchange_connections_per_peer'],
                        t['exchange_connecting_positions_per_peer'],
                        t['exchange_closing_positions_per_peer']),
        'runtime_filter': add(t['runtime_filter_connections_per_peer'],
                              t['runtime_filter_connecting_positions_per_peer'],
                              t['runtime_filter_closing_positions_per_peer']),
    })
    backends = t['maximum_live_backends']
    frontends = t['authenticated_live_frontends_per_backend']
    # Matches native_transport_geometry::envelope. BE peer directions use the
    # conservative configured B, while runtime placement uses the live registry.
    specifications = {
        'frontend': [*( (lane, 'outgoing', backends)
                       for lane in t['connections_per_frontend_backend']),
                     ('membership', 'incoming', backends)],
        'backend': [*( (lane, 'incoming', frontends)
                      for lane in t['connections_per_frontend_backend']),
                    ('exchange', 'incoming', backends),
                    ('runtime_filter', 'incoming', backends),
                    ('exchange', 'outgoing', backends),
                    ('runtime_filter', 'outgoing', backends),
                    ('membership', 'outgoing', frontends)],
    }
    roles = {}
    for role, specifications_for_role in specifications.items():
        lanes = []
        for lane, direction, peers in specifications_for_role:
            connections = mul(peers, per_peer[lane])
            send = (t['client_public_defaults']['send_buffer_bytes_per_stream']
                    if direction == 'outgoing' else t['h2_send_buffer_bytes'])
            stream_bytes = add(t['h2_stream_receive_window_bytes'], send,
                               t['h2_header_bytes'])
            streams = mul(connections, t['streams_per_connection'])
            lanes.append({
                'lane': lane, 'direction': direction, 'connections': connections,
                'streams': streams, 'stream_bytes': stream_bytes,
                'structural_bytes': mul(connections, add(
                    t['h2_connection_receive_window_bytes'],
                    mul(t['streams_per_connection'], stream_bytes))),
            })
        outgoing = add(*(lane['connections'] for lane in lanes
                         if lane['direction'] == 'outgoing'))
        handshakes = (mul(backends, mul(5, t['connecting_positions_per_lane']))
                      if role == 'frontend' else
                      add(t['data_handshake_positions'], t['control_handshake_positions']))
        roles[role] = {
            'lanes': lanes,
            'connections': add(*(lane['connections'] for lane in lanes)),
            'streams': add(*(lane['streams'] for lane in lanes)),
            'handshake_positions': handshakes,
            'queued_requests': mul(outgoing, t['tonic_pending_per_connection']),
            'structural_bytes': add(*(lane['structural_bytes'] for lane in lanes)),
            'coefficients': None,
            'total_bytes': None,
        }
    backend = roles['backend']
    root_encode = root_encode_envelope(profile, root_process_limit_bytes)
    backend['root_encode'] = root_encode
    backend['known_structural_bytes'] = add(
        backend['structural_bytes'], root_encode['encoding_requested_bytes'])
    return {
        'schema_version': 1,
        'status': 'STRUCTURAL_ARITHMETIC_ONLY_COEFFICIENTS_NOT_FROZEN',
        'source_contract': ('native_transport_geometry::envelope; '
                            'root_result_ingress_owner_limit; NativeResultSupportGeometry::V1'),
        'roles': roles,
    }


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--effective-root-process-limit-bytes', type=int,
                        help='Use a supplied effective BE Root process limit instead of the profile default')
    args = parser.parse_args()
    profile = json.loads((ROOT / 'profile-v1.json').read_text())
    try:
        report = envelope(profile, root_process_limit_bytes=args.effective_root_process_limit_bytes)
    except ValueError as error:
        parser.error(str(error))
    print(json.dumps(report, indent=2))
