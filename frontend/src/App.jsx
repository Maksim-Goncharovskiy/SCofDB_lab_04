import { useState, useEffect } from 'react'

const API_URL = '/api'

function App() {
  const [activeTab, setActiveTab] = useState('users')
  const [users, setUsers] = useState([])
  const [orders, setOrders] = useState([])
  const [error, setError] = useState(null)
  const [success, setSuccess] = useState(null)
  const [loading, setLoading] = useState(false)

  // User form state
  const [userEmail, setUserEmail] = useState('')
  const [userName, setUserName] = useState('')

  // Order form state
  const [selectedUserId, setSelectedUserId] = useState('')

  // Item form state
  const [selectedOrderId, setSelectedOrderId] = useState('')
  const [productName, setProductName] = useState('')
  const [productPrice, setProductPrice] = useState('')
  const [productQuantity, setProductQuantity] = useState('1')

  // Concurrent test state
  const [testOrderId, setTestOrderId] = useState('')
  const [testMode, setTestMode] = useState('unsafe')
  const [testResult, setTestResult] = useState(null)

  useEffect(() => {
    fetchUsers()
    fetchOrders()
  }, [])

  const showError = (msg) => {
    setError(msg)
    setSuccess(null)
    setTimeout(() => setError(null), 5000)
  }

  const showSuccess = (msg) => {
    setSuccess(msg)
    setError(null)
    setTimeout(() => setSuccess(null), 3000)
  }

  // API calls
  const fetchUsers = async () => {
    try {
      const res = await fetch(`${API_URL}/users`)
      if (res.ok) {
        setUsers(await res.json())
      }
    } catch (e) {
      console.error('Failed to fetch users:', e)
    }
  }

  const fetchOrders = async () => {
    try {
      const res = await fetch(`${API_URL}/orders`)
      if (res.ok) {
        setOrders(await res.json())
      }
    } catch (e) {
      console.error('Failed to fetch orders:', e)
    }
  }

  const createUser = async (e) => {
    e.preventDefault()
    setLoading(true)
    try {
      const res = await fetch(`${API_URL}/users`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email: userEmail, name: userName }),
      })
      if (res.ok) {
        showSuccess('User created successfully!')
        setUserEmail('')
        setUserName('')
        fetchUsers()
      } else {
        const data = await res.json()
        showError(data.detail || 'Failed to create user')
      }
    } catch (e) {
      showError('Network error')
    }
    setLoading(false)
  }

  const createOrder = async (e) => {
    e.preventDefault()
    if (!selectedUserId) {
      showError('Please select a user')
      return
    }
    setLoading(true)
    try {
      const res = await fetch(`${API_URL}/orders`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ user_id: selectedUserId }),
      })
      if (res.ok) {
        showSuccess('Order created successfully!')
        fetchOrders()
      } else {
        const data = await res.json()
        showError(data.detail || 'Failed to create order')
      }
    } catch (e) {
      showError('Network error')
    }
    setLoading(false)
  }

  const addItem = async (e) => {
    e.preventDefault()
    if (!selectedOrderId) {
      showError('Please select an order')
      return
    }
    setLoading(true)
    try {
      const res = await fetch(`${API_URL}/orders/${selectedOrderId}/items`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          product_name: productName,
          price: parseFloat(productPrice),
          quantity: parseInt(productQuantity),
        }),
      })
      if (res.ok) {
        showSuccess('Item added successfully!')
        setProductName('')
        setProductPrice('')
        setProductQuantity('1')
        fetchOrders()
      } else {
        const data = await res.json()
        showError(data.detail || 'Failed to add item')
      }
    } catch (e) {
      showError('Network error')
    }
    setLoading(false)
  }

  const payOrder = async (orderId) => {
    setLoading(true)
    try {
      const res = await fetch(`${API_URL}/orders/${orderId}/pay`, {
        method: 'POST',
      })
      if (res.ok) {
        showSuccess('Order paid successfully!')
        fetchOrders()
      } else {
        const data = await res.json()
        showError(data.detail || 'Failed to pay order')
      }
    } catch (e) {
      showError('Network error')
    }
    setLoading(false)
  }

  const cancelOrder = async (orderId) => {
    setLoading(true)
    try {
      const res = await fetch(`${API_URL}/orders/${orderId}/cancel`, {
        method: 'POST',
      })
      if (res.ok) {
        showSuccess('Order cancelled!')
        fetchOrders()
      } else {
        const data = await res.json()
        showError(data.detail || 'Failed to cancel order')
      }
    } catch (e) {
      showError('Network error')
    }
    setLoading(false)
  }

  // Concurrent payment testing
  const createTestOrder = async () => {
    setLoading(true)
    try {
      const userRes = await fetch(`${API_URL}/users`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          email: `test_${Date.now()}@example.com`,
          name: 'Test User for Concurrent Payment'
        }),
      })
      if (!userRes.ok) throw new Error('Failed to create user')
      const user = await userRes.json()
      
      const orderRes = await fetch(`${API_URL}/orders`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ user_id: user.id }),
      })
      if (!orderRes.ok) throw new Error('Failed to create order')
      const order = await orderRes.json()
      
      setTestOrderId(order.id)
      setTestResult(null)
      showSuccess(`Test order created: ${order.id.slice(0, 8)}...`)
      fetchOrders()
    } catch (e) {
      showError(e.message)
    }
    setLoading(false)
  }

  const testConcurrentPayment = async () => {
    if (!testOrderId) {
      showError('Create test order first!')
      return
    }
    setLoading(true)
    setTestResult(null)
    try {
      const res = await fetch(`${API_URL}/payments/test-concurrent`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          order_id: testOrderId,
          mode: testMode
        }),
      })
      if (res.ok) {
        const data = await res.json()
        setTestResult(data)
        fetchOrders()
      } else {
        const data = await res.json()
        showError(data.detail || 'Test failed')
      }
    } catch (e) {
      showError('Network error')
    }
    setLoading(false)
  }

  return (
    <div className="container">
      <h1>🛒 Marketplace</h1>
      
      {error && <div className="error">{error}</div>}
      {success && <div className="success">{success}</div>}

      <div className="tabs">
        <div 
          className={`tab ${activeTab === 'users' ? 'active' : ''}`}
          onClick={() => setActiveTab('users')}
        >
          Users
        </div>
        <div 
          className={`tab ${activeTab === 'orders' ? 'active' : ''}`}
          onClick={() => setActiveTab('orders')}
        >
          Orders
        </div>
        <div 
          className={`tab ${activeTab === 'test' ? 'active' : ''}`}
          onClick={() => setActiveTab('test')}
          style={{ background: activeTab === 'test' ? '#8b5cf6' : '', borderColor: '#8b5cf6' }}
        >
          🧪 Test Concurrent
        </div>
      </div>

      {activeTab === 'users' && (
        <div className="grid">
          <div className="card">
            <h2>Create User</h2>
            <form onSubmit={createUser}>
              <div className="form-group">
                <label>Email</label>
                <input
                  type="email"
                  value={userEmail}
                  onChange={(e) => setUserEmail(e.target.value)}
                  placeholder="user@example.com"
                  required
                />
              </div>
              <div className="form-group">
                <label>Name</label>
                <input
                  type="text"
                  value={userName}
                  onChange={(e) => setUserName(e.target.value)}
                  placeholder="John Doe"
                />
              </div>
              <button className="btn btn-primary" disabled={loading}>
                Create User
              </button>
            </form>
          </div>

          <div className="card">
            <h2>Users ({users.length})</h2>
            <table>
              <thead>
                <tr>
                  <th>Email</th>
                  <th>Name</th>
                </tr>
              </thead>
              <tbody>
                {users.map((user) => (
                  <tr key={user.id}>
                    <td>{user.email}</td>
                    <td>{user.name || '-'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {activeTab === 'orders' && (
        <>
          <div className="grid">
            <div className="card">
              <h2>Create Order</h2>
              <form onSubmit={createOrder}>
                <div className="form-group">
                  <label>User</label>
                  <select
                    value={selectedUserId}
                    onChange={(e) => setSelectedUserId(e.target.value)}
                    style={{ width: '100%', padding: '10px', borderRadius: '4px', border: '1px solid #ddd' }}
                  >
                    <option value="">Select user...</option>
                    {users.map((user) => (
                      <option key={user.id} value={user.id}>
                        {user.email}
                      </option>
                    ))}
                  </select>
                </div>
                <button className="btn btn-primary" disabled={loading}>
                  Create Order
                </button>
              </form>
            </div>

            <div className="card">
              <h2>Add Item to Order</h2>
              <form onSubmit={addItem}>
                <div className="form-group">
                  <label>Order</label>
                  <select
                    value={selectedOrderId}
                    onChange={(e) => setSelectedOrderId(e.target.value)}
                    style={{ width: '100%', padding: '10px', borderRadius: '4px', border: '1px solid #ddd' }}
                  >
                    <option value="">Select order...</option>
                    {orders.filter(o => o.status === 'created').map((order) => (
                      <option key={order.id} value={order.id}>
                        Order #{order.id.slice(0, 8)} - {order.total_amount}₽
                      </option>
                    ))}
                  </select>
                </div>
                <div className="form-group">
                  <label>Product Name</label>
                  <input
                    type="text"
                    value={productName}
                    onChange={(e) => setProductName(e.target.value)}
                    placeholder="iPhone 15"
                    required
                  />
                </div>
                <div className="form-group">
                  <label>Price</label>
                  <input
                    type="number"
                    step="0.01"
                    min="0"
                    value={productPrice}
                    onChange={(e) => setProductPrice(e.target.value)}
                    placeholder="99999"
                    required
                  />
                </div>
                <div className="form-group">
                  <label>Quantity</label>
                  <input
                    type="number"
                    min="1"
                    value={productQuantity}
                    onChange={(e) => setProductQuantity(e.target.value)}
                    required
                  />
                </div>
                <button className="btn btn-primary" disabled={loading}>
                  Add Item
                </button>
              </form>
            </div>
          </div>

          <div className="card">
            <h2>Orders ({orders.length})</h2>
            {orders.map((order) => (
              <div key={order.id} className="card" style={{ background: '#fafafa' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <div>
                    <strong>Order #{order.id.slice(0, 8)}</strong>
                    <span className={`status-badge status-${order.status}`} style={{ marginLeft: '12px' }}>
                      {order.status}
                    </span>
                  </div>
                  <div>
                    <strong>{order.total_amount}₽</strong>
                  </div>
                </div>
                
                {order.items && order.items.length > 0 && (
                  <div className="order-items">
                    {order.items.map((item) => (
                      <div key={item.id} className="order-item">
                        <span>{item.product_name} × {item.quantity}</span>
                        <span>{item.subtotal}₽</span>
                      </div>
                    ))}
                  </div>
                )}

                <div className="actions" style={{ marginTop: '12px' }}>
                  {order.status === 'created' && (
                    <>
                      <button 
                        className="btn btn-success" 
                        onClick={() => payOrder(order.id)}
                        disabled={loading}
                      >
                        Pay
                      </button>
                      <button 
                        className="btn btn-danger" 
                        onClick={() => cancelOrder(order.id)}
                        disabled={loading}
                      >
                        Cancel
                      </button>
                    </>
                  )}
                  {order.status === 'paid' && (
                    <span style={{ color: '#666' }}>✓ Paid - awaiting shipment</span>
                  )}
                </div>
              </div>
            ))}
          </div>
        </>
      )}

      {activeTab === 'test' && (
        <div>
          <div className="card" style={{ background: '#fef3c7', borderColor: '#f59e0b' }}>
            <h3 style={{ margin: 0, color: '#92400e' }}>⚠️ Демонстрационная страница</h3>
            <p style={{ marginBottom: 0, color: '#78350f', fontSize: '14px' }}>
              Эта страница позволяет протестировать два режима оплаты:<br/>
              • <strong>unsafe</strong> - ломается при конкурентных запросах (READ COMMITTED без FOR UPDATE)<br/>
              • <strong>safe</strong> - работает корректно (REPEATABLE READ + FOR UPDATE)
            </p>
          </div>

          <div className="grid">
            <div className="card">
              <h2>Шаг 1: Создать тестовый заказ</h2>
              <button 
                className="btn btn-primary" 
                onClick={createTestOrder}
                disabled={loading}
              >
                ➕ Создать тестовый заказ
              </button>
              {testOrderId && (
                <div style={{ marginTop: '12px', padding: '12px', background: '#d1fae5', borderRadius: '4px' }}>
                  <strong style={{ color: '#065f46' }}>✅ Заказ создан:</strong>
                  <code style={{ marginLeft: '8px', background: '#a7f3d0', padding: '4px 8px', borderRadius: '4px' }}>
                    {testOrderId.slice(0, 8)}...
                  </code>
                </div>
              )}
            </div>

            <div className="card">
              <h2>Шаг 2: Запустить тест</h2>
              <div className="form-group">
                <label>Order ID:</label>
                <input
                  type="text"
                  value={testOrderId}
                  onChange={(e) => setTestOrderId(e.target.value)}
                  placeholder="Введите Order ID или создайте новый"
                />
              </div>

              <div className="form-group">
                <label>Режим тестирования:</label>
                <div style={{ display: 'flex', gap: '16px' }}>
                  <label style={{ display: 'flex', alignItems: 'center', cursor: 'pointer' }}>
                    <input
                      type="radio"
                      value="unsafe"
                      checked={testMode === 'unsafe'}
                      onChange={(e) => setTestMode(e.target.value)}
                      style={{ marginRight: '8px' }}
                    />
                    <span style={{ color: '#dc2626', fontWeight: 'bold' }}>❌ Unsafe (с проблемой)</span>
                  </label>
                  <label style={{ display: 'flex', alignItems: 'center', cursor: 'pointer' }}>
                    <input
                      type="radio"
                      value="safe"
                      checked={testMode === 'safe'}
                      onChange={(e) => setTestMode(e.target.value)}
                      style={{ marginRight: '8px' }}
                    />
                    <span style={{ color: '#16a34a', fontWeight: 'bold' }}>✅ Safe (без проблемы)</span>
                  </label>
                </div>
              </div>

              <button 
                className="btn" 
                style={{ background: '#8b5cf6', color: 'white' }}
                onClick={testConcurrentPayment}
                disabled={loading || !testOrderId}
              >
                🚀 Запустить 2 параллельные оплаты
              </button>
            </div>
          </div>

          {testResult && (
            <div className="card">
              <h2>Результаты теста</h2>
              
              <div 
                className="card" 
                style={{ 
                  background: testResult.summary.race_condition_detected ? '#fee2e2' : '#d1fae5',
                  borderColor: testResult.summary.race_condition_detected ? '#dc2626' : '#16a34a'
                }}
              >
                <h3 style={{ 
                  margin: 0, 
                  color: testResult.summary.race_condition_detected ? '#991b1b' : '#065f46',
                  fontSize: '18px'
                }}>
                  {testResult.explanation}
                </h3>
              </div>

              <div className="grid" style={{ gridTemplateColumns: 'repeat(4, 1fr)', marginTop: '16px' }}>
                <div className="card" style={{ background: '#f3f4f6', textAlign: 'center' }}>
                  <div style={{ fontSize: '32px', fontWeight: 'bold', color: '#374151' }}>
                    {testResult.summary.total_attempts}
                  </div>
                  <div style={{ fontSize: '14px', color: '#6b7280' }}>Попыток оплаты</div>
                </div>
                <div className="card" style={{ background: '#d1fae5', textAlign: 'center' }}>
                  <div style={{ fontSize: '32px', fontWeight: 'bold', color: '#16a34a' }}>
                    {testResult.summary.successful}
                  </div>
                  <div style={{ fontSize: '14px', color: '#6b7280' }}>Успешных</div>
                </div>
                <div className="card" style={{ background: '#fee2e2', textAlign: 'center' }}>
                  <div style={{ fontSize: '32px', fontWeight: 'bold', color: '#dc2626' }}>
                    {testResult.summary.failed}
                  </div>
                  <div style={{ fontSize: '14px', color: '#6b7280' }}>Неудачных</div>
                </div>
                <div className="card" style={{ background: '#dbeafe', textAlign: 'center' }}>
                  <div style={{ fontSize: '32px', fontWeight: 'bold', color: '#1e40af' }}>
                    {testResult.summary.payment_count_in_history}
                  </div>
                  <div style={{ fontSize: '14px', color: '#6b7280' }}>В истории БД</div>
                </div>
              </div>

              <div style={{ marginTop: '16px' }}>
                <h3>Детали попыток:</h3>
                {testResult.results.map((attempt, i) => (
                  <div 
                    key={i}
                    className="card"
                    style={{
                      background: attempt.success ? '#d1fae5' : '#fee2e2',
                      borderColor: attempt.success ? '#16a34a' : '#dc2626',
                      marginBottom: '8px'
                    }}
                  >
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                      <span style={{ fontWeight: 'bold' }}>Попытка {attempt.attempt}</span>
                      <span 
                        style={{
                          padding: '4px 12px',
                          borderRadius: '4px',
                          fontSize: '14px',
                          fontWeight: 'bold',
                          background: attempt.success ? '#16a34a' : '#dc2626',
                          color: 'white'
                        }}
                      >
                        {attempt.success ? '✅ Успешно' : '❌ Ошибка'}
                      </span>
                    </div>
                    {attempt.error && (
                      <div style={{ marginTop: '8px', fontSize: '14px', color: '#991b1b' }}>
                        <code>{attempt.error}</code>
                      </div>
                    )}
                  </div>
                ))}
              </div>

              {testResult.history && testResult.history.length > 0 && (
                <div style={{ marginTop: '16px' }}>
                  <h3>История оплат в БД:</h3>
                  <table>
                    <thead>
                      <tr>
                        <th>ID</th>
                        <th>Status</th>
                        <th>Timestamp</th>
                      </tr>
                    </thead>
                    <tbody>
                      {testResult.history.map((record) => (
                        <tr key={record.id}>
                          <td>{record.id.slice(0, 8)}...</td>
                          <td>
                            <span className="status-badge status-paid">{record.status}</span>
                          </td>
                          <td>{new Date(record.changed_at).toLocaleString()}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          )}

          <div className="card" style={{ background: '#eff6ff', borderColor: '#3b82f6' }}>
            <h3 style={{ marginTop: 0, color: '#1e40af' }}>📖 Как использовать:</h3>
            <ol style={{ color: '#1e3a8a', fontSize: '14px' }}>
              <li>Создайте тестовый заказ</li>
              <li>Выберите режим: <strong>unsafe</strong> (демонстрация проблемы) или <strong>safe</strong> (решение)</li>
              <li>Нажмите "Запустить 2 параллельные оплаты"</li>
              <li>Проверьте результаты:
                <ul style={{ marginTop: '4px' }}>
                  <li><strong>Unsafe:</strong> заказ будет оплачен дважды ⚠️</li>
                  <li><strong>Safe:</strong> заказ будет оплачен только один раз ✅</li>
                </ul>
              </li>
            </ol>
          </div>
        </div>
      )}
    </div>
  )
}

export default App
