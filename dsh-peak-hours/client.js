window.__ModuleLoader__.load({
  id: '@local/dsh-peak-hours',
  factory(require) {
    let React = null
    try {
      React = require('react')
    } catch (error) {
      void error
    }
    const h = (...args) => (React ? React.createElement(...args) : null)

    const LOCALE_NS = 'peak-hours'
    const MSK_OFFSET_MINUTES = 180
    // Peak windows are defined in UTC: 01:00-04:00 and 06:00-10:00.
    const UTC_WINDOWS = [
      { start: 60, end: 240 },
      { start: 360, end: 600 },
    ]
    const REFRESH_MS = 15000

    const RU = {
      badgePrefix: 'Пик',
      msk: 'МСК',
      tipSchedule: 'Пиковые часы по МСК: 04:00–07:00 и 09:00–13:00 (01:00–04:00 и 06:00–10:00 UTC).',
      tipNow: 'Сейчас идёт окно',
      tipLeft: 'осталось',
      leftHours: 'ч',
      leftMinutes: 'мин',
    }
    const EN = {
      badgePrefix: 'Peak',
      msk: 'MSK',
      tipSchedule: 'Peak hours in Moscow time: 04:00–07:00 and 09:00–13:00 (01:00–04:00 and 06:00–10:00 UTC).',
      tipNow: 'Current window:',
      tipLeft: 'left',
      leftHours: 'h',
      leftMinutes: 'min',
    }

    const CSS = [
      '.dsh-peak-hours{display:inline-flex;align-items:center;gap:6px;height:22px;box-sizing:border-box;margin:0 4px;padding:0 8px;border:1px solid var(--dsw-alias-border-l2);border-radius:6px;background:var(--dsw-alias-bg-layer-2);color:var(--dsw-alias-label-secondary);font-size:11px;font-weight:600;line-height:1;white-space:nowrap;font-variant-numeric:tabular-nums;cursor:default;user-select:none;flex:none}',
      '.dsh-peak-hours__dot{display:inline-block;width:6px;height:6px;border-radius:50%;flex:none;background:var(--dsw-alias-state-warn-primary)}',
      '.dsh-peak-hours__icon{display:block;width:12px;height:12px;flex:none;color:var(--dsw-alias-state-warn-primary)}',
      '.dsh-peak-hours__text{display:inline-block}',
      '@media (max-width:720px){.dsh-peak-hours__text{display:none}}',
    ].join('')

    const pad2 = (value) => (value < 10 ? '0' + value : String(value))
    const mskClock = (utcMinutes) => {
      const total = (utcMinutes + MSK_OFFSET_MINUTES) % 1440
      return pad2(Math.floor(total / 60)) + ':' + pad2(total % 60)
    }
    const windowLabel = (window) => mskClock(window.start) + '–' + mskClock(window.end)

    const activeWindow = (now) => {
      const utcMinutes = now.getUTCHours() * 60 + now.getUTCMinutes()
      for (const window of UTC_WINDOWS) {
        if (utcMinutes >= window.start && utcMinutes < window.end) {
          const remainingMinutes = window.end - utcMinutes
          return { window, remainingMinutes }
        }
      }
      return null
    }

    const formatRemaining = (minutes, t) => {
      const hours = Math.floor(minutes / 60)
      const rest = minutes % 60
      const parts = []
      if (hours > 0) parts.push(hours + ' ' + t('leftHours'))
      if (rest > 0 || parts.length === 0) parts.push(rest + ' ' + t('leftMinutes'))
      return parts.join(' ')
    }

    const buildTooltip = (active, t) => {
      const label = windowLabel(active.window) + ' ' + t('msk')
      return t('tipSchedule') + ' ' + t('tipNow') + ' ' + label + ', ' +
        t('tipLeft') + ' ' + formatRemaining(active.remainingMinutes, t) + '.'
    }

    const makeTranslator = (props) => {
      const injected = props && typeof props.t === 'function' ? props.t : null
      return (key) => {
        if (injected) {
          try {
            const value = injected(key)
            if (typeof value === 'string' && value && value !== key) return value
          } catch (error) {
            void error
          }
        }
        return RU[key] || key
      }
    }

    function PeakHoursBadge(props) {
      const t = makeTranslator(props)
      const [now, setNow] = React.useState(() => new Date())
      React.useEffect(() => {
        const timer = setInterval(() => setNow(new Date()), REFRESH_MS)
        return () => clearInterval(timer)
      }, [])
      const active = activeWindow(now)
      if (!active) return null
      const label = t('badgePrefix') + ' ' + windowLabel(active.window) + ' ' + t('msk')
      const tooltip = buildTooltip(active, t)
      return h('span', {
        className: 'dsh-peak-hours',
        title: tooltip,
        'aria-label': tooltip,
      },
        h('svg', {
          className: 'dsh-peak-hours__icon',
          viewBox: '0 0 16 16',
          'aria-hidden': true,
        }, h('path', { d: 'M9.6 1 3.4 9.1h3.1L6 15l6.6-8.5H9.4L9.6 1Z', fill: 'currentColor' })),
        h('span', { className: 'dsh-peak-hours__text' }, label),
      )
    }

    function apply(ctx) {
      const slots = ctx && ctx.slots
      if (!slots) return

      if (ctx.locale && typeof ctx.locale.register === 'function') {
        ctx.effect(() => {
          const disposers = []
          try {
            disposers.push(ctx.locale.register(LOCALE_NS, 'ru', RU))
          } catch (error) {
            void error
          }
          try {
            disposers.push(ctx.locale.register(LOCALE_NS, 'en', EN))
          } catch (error) {
            void error
          }
          return () => {
            for (const dispose of disposers) {
              try {
                if (typeof dispose === 'function') dispose()
              } catch (error) {
                void error
              }
            }
          }
        }, 'dsh-peak-hours: locale')
      }

      ctx.effect(() => {
        if (typeof document === 'undefined' || !document.head) return () => {}
        const tag = document.createElement('style')
        tag.dataset.dshPlugin = 'dsh-peak-hours'
        tag.textContent = CSS
        document.head.appendChild(tag)
        return () => {
          if (tag.parentNode) tag.parentNode.removeChild(tag)
        }
      }, 'dsh-peak-hours: styles')

      ctx.effect(() => slots.inject('conversation.session.header.utilities', () =>
        slots.register({
          name: 'conversation.session.header.utilities',
          id: 'peak-hours',
          order: 90,
          locale: LOCALE_NS,
          label: () => 'Peak hours',
        }, PeakHoursBadge),
      ), 'dsh-peak-hours: header badge')
    }

    return { apply, inject: ['slots', 'locale'] }
  },
})
