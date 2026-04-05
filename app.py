from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
from psycopg2.pool import SimpleConnectionPool
import os
import json

app = Flask(__name__)
CORS(app)

# Database connection pool
db_url = os.getenv('DATABASE_PUBLIC_URL')
pool = SimpleConnectionPool(1, 20, db_url)

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok'})

@app.route('/api/detections', methods=['POST'])
def save_detection():
    try:
        data = request.json
        image_id = data.get('image_id')
        detections = data.get('detections')
        labels = data.get('labels')
        
        if not image_id or not detections:
            return jsonify({'error': 'Missing required fields: image_id, detections'}), 400
        
        conn = pool.getconn()
        cur = conn.cursor()
        
        query = """
            INSERT INTO detections (image_id, detections, labels, created_at)
            VALUES (%s, %s, %s, NOW())
            RETURNING id, image_id, created_at;
        """
        
        cur.execute(query, (image_id, json.dumps(detections), json.dumps(labels) if labels else None))
        result = cur.fetchone()
        conn.commit()
        cur.close()
        pool.putconn(conn)
        
        return jsonify({
            'success': True,
            'message': 'Detection saved successfully',
            'data': {'id': result[0], 'image_id': result[1], 'created_at': str(result[2])}
        })
    except Exception as e:
        return jsonify({'error': 'Failed to save detection', 'details': str(e)}), 500

@app.route('/api/detections/batch', methods=['POST'])
def save_batch():
    try:
        data = request.json
        items = data.get('items', [])
        
        if not isinstance(items, list) or len(items) == 0:
            return jsonify({'error': 'items must be a non-empty array'}), 400
        
        conn = pool.getconn()
        cur = conn.cursor()
        
        results = []
        errors = []
        
        for i, item in enumerate(items):
            try:
                image_id = item.get('image_id')
                detections = item.get('detections')
                labels = item.get('labels')
                
                if not image_id or not detections:
                    errors.append({'index': i, 'error': 'Missing required fields: image_id, detections'})
                    continue
                
                query = """
                    INSERT INTO detections (image_id, detections, labels, created_at)
                    VALUES (%s, %s, %s, NOW())
                    RETURNING id, image_id, created_at;
                """
                
                cur.execute(query, (image_id, json.dumps(detections), json.dumps(labels) if labels else None))
                result = cur.fetchone()
                results.append({'index': i, 'success': True, 'data': {'id': result[0], 'image_id': result[1], 'created_at': str(result[2])}})
            except Exception as e:
                errors.append({'index': i, 'error': str(e)})
        
        conn.commit()
        cur.close()
        pool.putconn(conn)
        
        return jsonify({
            'success': len(errors) == 0,
            'saved': len(results),
            'failed': len(errors),
            'results': results,
            'errors': errors if errors else None
        })
    except Exception as e:
        return jsonify({'error': 'Batch operation failed', 'details': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
